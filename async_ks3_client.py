"""
异步KS3客户端
基于aiohttp实现完全异步的金山云KS3对象存储客户端
支持分片上传、并发处理和高性能文件上传
"""

import hashlib
import hmac
import base64
import urllib.parse
from datetime import datetime
import aiohttp
import aiofiles
import asyncio
import logging
from typing import Dict, List, Optional, Tuple, AsyncGenerator
import xml.etree.ElementTree as ET
from django.conf import settings

logger = logging.getLogger(__name__)


class KS3Signature:
    """KS3认证签名算法实现"""
    
    @staticmethod
    def generate_signature(
        access_key: str,
        secret_key: str, 
        method: str,
        content_md5: str,
        content_type: str,
        date: str,
        canonicalized_ks3_headers: str,
        canonicalized_resource: str
    ) -> str:
        """
        生成KS3认证签名
        
        Args:
            access_key: 访问密钥ID
            secret_key: 访问密钥
            method: HTTP方法 (GET, POST, PUT, DELETE)
            content_md5: 内容MD5值
            content_type: 内容类型
            date: RFC822格式的日期
            canonicalized_ks3_headers: 规范化的KS3头部
            canonicalized_resource: 规范化的资源路径
        
        Returns:
            签名字符串
        """
        # 构建待签名字符串
        string_to_sign = f"{method}\n{content_md5}\n{content_type}\n{date}\n{canonicalized_ks3_headers}{canonicalized_resource}"
        
        # 使用HMAC-SHA1生成签名
        signature = base64.b64encode(
            hmac.new(
                secret_key.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha1
            ).digest()
        ).decode('utf-8')
        
        return f"KSS {access_key}:{signature}"
    
    @staticmethod
    def canonicalize_ks3_headers(headers: Dict[str, str]) -> str:
        """规范化KS3头部"""
        ks3_headers = {}
        for key, value in headers.items():
            lower_key = key.lower()
            if lower_key.startswith('x-kss-'):
                ks3_headers[lower_key] = str(value).strip()
        
        if not ks3_headers:
            return ""
        
        # 按字典序排序并拼接
        sorted_headers = sorted(ks3_headers.items())
        return ''.join(f"{key}:{value}\n" for key, value in sorted_headers)
    
    @staticmethod
    def canonicalize_resource(bucket: str, object_key: str, query_params: Dict[str, str] = None) -> str:
        """规范化资源路径"""
        resource = f"/{bucket}"
        if object_key:
            resource += f"/{object_key}"
        
        if query_params:
            # 特殊查询参数需要包含在签名中
            special_params = ['uploadId', 'partNumber', 'uploads', 'delete', 'cors', 'logging', 'website', 'lifecycle', 'notification']
            included_params = {k: v for k, v in query_params.items() if k in special_params}
            if included_params:
                sorted_params = sorted(included_params.items())
                query_string = '&'.join(f"{k}={v}" if v else k for k, v in sorted_params)
                resource += f"?{query_string}"
        
        return resource


class AsyncKS3Client:
    """异步KS3客户端"""
    
    def __init__(self, access_key: str, secret_key: str, endpoint: str, bucket: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint
        self.bucket = bucket
        self.base_url = f"https://{bucket}.{endpoint}"
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建aiohttp会话"""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=100,  # 连接池大小
                limit_per_host=20,  # 每个主机的连接数
                ttl_dns_cache=300,  # DNS缓存TTL
                use_dns_cache=True,
                keepalive_timeout=60,  # 保持连接时间
                enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(
                total=300,  # 总超时时间5分钟
                connect=30,  # TCP握手超时30秒
                sock_connect=300,  # Socket连接超时5分钟
                sock_read=60  # Socket读取超时60秒
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': 'AsyncKS3Client/1.0'}
            )
        return self._session
    
    async def close(self):
        """关闭客户端会话"""
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _build_headers(
        self,
        method: str,
        object_key: str = "",
        content_type: str = "",
        content_md5: str = "",
        extra_headers: Dict[str, str] = None,
        query_params: Dict[str, str] = None
    ) -> Dict[str, str]:
        """构建请求头，包含认证签名"""
        
        # 当前时间
        date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        
        # 基础头部
        headers = {
            'Date': date,
            'Host': f"{self.bucket}.{self.endpoint}"
        }
        
        if content_type:
            headers['Content-Type'] = content_type
        if content_md5:
            headers['Content-MD5'] = content_md5
        if extra_headers:
            headers.update(extra_headers)
        
        # 规范化头部和资源
        canonicalized_headers = KS3Signature.canonicalize_ks3_headers(headers)
        canonicalized_resource = KS3Signature.canonicalize_resource(
            self.bucket, object_key, query_params
        )
        
        # 生成签名
        authorization = KS3Signature.generate_signature(
            self.access_key,
            self.secret_key,
            method,
            content_md5,
            content_type,
            date,
            canonicalized_headers,
            canonicalized_resource
        )
        
        headers['Authorization'] = authorization
        return headers
    
    async def initiate_multipart_upload(
        self,
        object_key: str,
        content_type: str = "application/octet-stream",
        storage_class: str = "STANDARD"
    ) -> str:
        """
        初始化分片上传
        
        Args:
            object_key: 对象键名
            content_type: 内容类型
            storage_class: 存储类型 (STANDARD, STANDARD_IA)
        
        Returns:
            uploadId: 上传ID
        """
        query_params = {'uploads': ''}
        extra_headers = {
            'x-kss-storage-class': storage_class,
            'x-kss-acl': 'public-read',  # 设置公共读权限
            'Cache-Control': 'public, max-age=31536000'
        }
        
        headers = self._build_headers(
            'POST', object_key, content_type, '', extra_headers, query_params
        )
        
        url = f"{self.base_url}/{object_key}?uploads"
        session = await self._get_session()
        
        async with session.post(url, headers=headers) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"初始化分片上传HTTP错误: status={response.status}, headers={dict(response.headers)}, body={error_text}")
                raise Exception(f"初始化分片上传失败: {response.status}, {error_text}")
            
            # 解析XML响应获取uploadId
            xml_content = await response.text()
            logger.info(f"初始化分片上传响应: {xml_content}")  # 临时改为info级别
            
            try:
                root = ET.fromstring(xml_content)
                upload_id_element = root.find('UploadId')
                
                if upload_id_element is None:
                    # 尝试查找不同的命名空间
                    upload_id_element = root.find('.//{*}UploadId')
                
                if upload_id_element is None or upload_id_element.text is None:
                    raise Exception(f"XML响应中找不到UploadId元素: {xml_content}")
                
                upload_id = upload_id_element.text
                logger.info(f"分片上传初始化成功: {object_key}, uploadId: {upload_id}")
                return upload_id
                
            except ET.ParseError as parse_error:
                raise Exception(f"XML解析失败: {parse_error}, 响应内容: {xml_content}")
            except Exception as e:
                raise Exception(f"解析uploadId失败: {e}, 响应内容: {xml_content}")
    
    async def upload_part(
        self,
        object_key: str,
        upload_id: str,
        part_number: int,
        data: bytes
    ) -> str:
        """
        上传单个分片
        
        Args:
            object_key: 对象键名
            upload_id: 上传ID
            part_number: 分片号 (1-10000)
            data: 分片数据
        
        Returns:
            etag: 分片的ETag值
        """
        query_params = {
            'uploadId': upload_id,
            'partNumber': str(part_number)
        }
        
        # 计算MD5
        content_md5 = base64.b64encode(hashlib.md5(data).digest()).decode()
        
        headers = self._build_headers(
            'PUT', object_key, 'application/octet-stream', content_md5, {}, query_params
        )
        
        url = f"{self.base_url}/{object_key}?uploadId={upload_id}&partNumber={part_number}"
        session = await self._get_session()
        
        async with session.put(url, headers=headers, data=data) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"分片上传失败: part {part_number}, {response.status}, {error_text}")
            
            etag = response.headers.get('ETag', '').strip('"')
            logger.debug(f"分片上传成功: part {part_number}, etag: {etag}")
            return etag
    
    async def complete_multipart_upload(
        self,
        object_key: str,
        upload_id: str,
        parts: List[Tuple[int, str]]  # [(part_number, etag), ...]
    ) -> str:
        """
        完成分片上传
        
        Args:
            object_key: 对象键名
            upload_id: 上传ID
            parts: 分片列表 [(分片号, ETag), ...]
        
        Returns:
            object_url: 对象访问URL
        """
        query_params = {'uploadId': upload_id}
        
        # 构建XML请求体
        root = ET.Element('CompleteMultipartUpload')
        for part_number, etag in sorted(parts):
            part_elem = ET.SubElement(root, 'Part')
            part_number_elem = ET.SubElement(part_elem, 'PartNumber')
            part_number_elem.text = str(part_number)
            etag_elem = ET.SubElement(part_elem, 'ETag')
            etag_elem.text = etag
        
        xml_data = ET.tostring(root, encoding='utf-8')
        content_md5 = base64.b64encode(hashlib.md5(xml_data).digest()).decode()
        
        headers = self._build_headers(
            'POST', object_key, 'application/xml', content_md5, {}, query_params
        )
        
        url = f"{self.base_url}/{object_key}?uploadId={upload_id}"
        session = await self._get_session()
        
        async with session.post(url, headers=headers, data=xml_data) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"完成分片上传HTTP错误: status={response.status}, headers={dict(response.headers)}, body={error_text}")
                raise Exception(f"完成分片上传失败: {response.status}, {error_text}")
            
            # 解析响应获取对象URL
            xml_content = await response.text()
            logger.info(f"完成分片上传响应: {xml_content}")  # 临时改为info级别
            
            try:
                root = ET.fromstring(xml_content)
                location_element = root.find('Location')
                
                if location_element is None:
                    # 尝试查找不同的命名空间
                    location_element = root.find('.//{*}Location')
                
                if location_element is None or location_element.text is None:
                    raise Exception(f"XML响应中找不到Location元素: {xml_content}")
                
                location = location_element.text
                logger.info(f"分片上传完成: {object_key}, URL: {location}")
                return location
                
            except ET.ParseError as parse_error:
                raise Exception(f"XML解析失败: {parse_error}, 响应内容: {xml_content}")
            except Exception as e:
                raise Exception(f"解析Location失败: {e}, 响应内容: {xml_content}")
    
    async def abort_multipart_upload(self, object_key: str, upload_id: str):
        """取消分片上传"""
        query_params = {'uploadId': upload_id}
        headers = self._build_headers('DELETE', object_key, '', '', {}, query_params)
        
        url = f"{self.base_url}/{object_key}?uploadId={upload_id}"
        session = await self._get_session()
        
        async with session.delete(url, headers=headers) as response:
            if response.status not in [200, 204]:
                error_text = await response.text()
                logger.warning(f"取消分片上传失败: {response.status}, {error_text}")
            else:
                logger.info(f"分片上传已取消: {object_key}, uploadId: {upload_id}")


# 全局客户端实例
_ks3_client: Optional[AsyncKS3Client] = None


async def get_async_ks3_client() -> AsyncKS3Client:
    """获取全局异步KS3客户端实例"""
    global _ks3_client
    if _ks3_client is None:
        _ks3_client = AsyncKS3Client(
            access_key=settings.KS3_ACCESS_KEY,
            secret_key=settings.KS3_SECRET_KEY,
            endpoint=settings.KS3_ENDPOINT,
            bucket=settings.KS3_BUCKET
        )
    return _ks3_client


async def close_async_ks3_client():
    """关闭全局异步KS3客户端"""
    global _ks3_client
    if _ks3_client:
        await _ks3_client.close()
        _ks3_client = None 

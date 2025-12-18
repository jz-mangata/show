"""
用户余额不足统一处理工具
提供一致的余额检查和处理逻辑
"""

import logging
import math
from typing import Optional, Tuple, Dict, Any
from django.db.models import F
from web import models

logger = logging.getLogger(__name__)


async def check_and_handle_insufficient_balance(
    user: models.User,
    tokens: int,
    usage_type: str = "ai回复",
    store: Optional[models.StoreInformation] = None
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """
    检查用户余额并处理余额不足情况

    Args:
        user: 用户对象
        tokens: 本次消耗的token数量
        usage_type: 使用类型 ("ai回复", "标题生成", "知识库修改" 等)
        store: 店铺对象 (可选，用于获取店铺信息)

    Returns:
        Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        - bool: True表示余额充足，False表示余额不足
        - str: 余额不足时的错误消息 (给客户看的友好消息)
        - dict: 余额不足时的详细信息 (给系统/客服看的)
    """

    # 计算需要扣除的条数
    token_cost = math.ceil(tokens / 1000) if tokens > 0 else 0

    billing_strategy = await _get_billing_strategy(user)

    if (
        billing_strategy.get("is_partner_subordinate")
        and billing_strategy.get("billing_mode") == "partner_only"
        and billing_strategy.get("partner_user") is not None
    ):
        partner_user = billing_strategy["partner_user"]

        if partner_user.surplus_number >= token_cost:
            return True, None, None

        error_info: Dict[str, Any] = {
            "status": "insufficient_balance",
            "message": "合作商余额不足",
            "required": token_cost,
            "partner_available": partner_user.surplus_number,
            "billing_mode": "partner_only",
        }

        await _handle_partner_insufficient_balance(
            user, partner_user, token_cost, usage_type, error_info
        )

        customer_message = _get_customer_friendly_message(usage_type)
        return False, customer_message, error_info

    if user.surplus_number >= token_cost:
        return True, None, None

    logger.warning(
        f"⚠️ 用户余额不足 - 用户: {user.username}, 需要: {token_cost}, "
        f"剩余: {user.surplus_number}, 类型: {usage_type}"
    )

    result_info = await _handle_insufficient_balance_internal(
        user, token_cost, tokens, usage_type, store
    )

    customer_message = _get_customer_friendly_message(usage_type)
    return False, customer_message, result_info


async def _handle_insufficient_balance_internal(
    user: models.User,
    token_cost: int,
    tokens: int,
    usage_type: str,
    store: Optional[models.StoreInformation]
) -> Dict[str, Any]:
    """内部余额不足处理逻辑"""

    result_info = {
        "user_id": user.pk,
        "username": user.username,
        "original_balance": user.surplus_number,
        "required_cost": token_cost,
        "tokens": tokens,
        "usage_type": usage_type,
        "closed_stores": []
    }

    try:
        # 1. 记录剩余条数使用详情（如果还有剩余条数）
        if user.surplus_number > 0:
            await models.AIUsageDetails(
                use_number=user.surplus_number,
                surplus=0,
                use_type=usage_type,
                user=user,
                tokens=tokens,
                consumer=user,
                store=store if usage_type not in ["充值", "购买合作商套餐", "套餐赠送"] else None,
            ).asave()
            logger.info(f"📊 记录使用详情 - 用完剩余{user.surplus_number}条")

        # 2. 清零用户条数
        user.surplus_number = 0
        await user.asave()
        logger.info(f"💰 用户条数已清零 - 用户: {user.username}")

        # 3. 不再自动关闭智能接待（根据业务需求调整）
        result_info["closed_stores"] = []

        # 4. 创建系统通知
        await models.SystemNotify(
            title=_get_notification_title(usage_type),
            content=_get_notification_content(usage_type, 0),
            notify_type="系统通知",
            user=user,
        ).asave()

        logger.info(f"📢 系统通知已创建 - 用户: {user.username}, 类型: {usage_type}")

        result_info["status"] = "success"
        result_info["message"] = "余额不足处理完成"

    except Exception as e:
        logger.error(f"❌ 余额不足处理异常 - 用户: {user.username}, 错误: {e}")
        result_info["status"] = "error"
        result_info["message"] = f"处理异常: {str(e)}"

    return result_info


def _get_customer_friendly_message(usage_type: str) -> str:
    """获取给客户看的友好错误消息"""
    messages = {
        "ai回复": "抱歉，当前客服繁忙",
        "标题生成": "剩余AI回复条数不足，请检查剩余条数",
        "知识库修改": "剩余AI回复条数不足，请检查剩余条数",
        "商品参数提取": "剩余AI回复条数不足，请检查剩余条数",
    }
    return messages.get(usage_type, "剩余AI回复条数不足，请检查剩余条数")


def _get_notification_title(usage_type: str) -> str:
    """获取系统通知标题"""
    titles = {
        "ai回复": "AI回复条数不足",
        "标题生成": "AI回复条数不足",
        "知识库修改": "AI回复条数不足",
        "商品参数提取": "AI回复条数不足",
    }
    return titles.get(usage_type, "AI回复条数不足")


def _get_notification_content(usage_type: str, closed_stores_count: int) -> str:
    """获取系统通知内容"""
    base_content = """尊敬的顾客
您的账户余额已不足，请购买条数或续费套餐。续费后，请重启水滴智能通讯插件"""

    return base_content


# 兼容性函数：简化版本，仅检查余额（按合作商计费策略）
async def check_balance_sufficient(user: models.User, tokens: int) -> tuple[bool, str]:
    """按当前计费策略检查余额是否充足（不执行扣费和处理逻辑）
    
    Returns:
        tuple[bool, str]: (是否充足, 错误信息)
        - 充足时: (True, "")
        - 不足时: (False, "具体的错误提示")
    """

    token_cost = math.ceil(tokens / 1000) if tokens > 0 else 0
    if token_cost == 0:
        return True, ""

    # 获取计费策略，统一兼容合作商和包月场景
    billing_strategy = await _get_billing_strategy(user)

    # 合作商下级用户
    if billing_strategy.get("is_partner_subordinate"):
        partner_user = billing_strategy.get("partner_user")
        billing_mode = billing_strategy.get("billing_mode")

        # 防御：如果没有有效的合作商信息，退回到普通用户检查
        if partner_user is None:
            if user.surplus_number >= token_cost:
                return True, ""
            else:
                return False, "您的余额不足，请充值"

        # 仅扣合作商（包月权益）
        if billing_mode == "partner_only":
            if partner_user.surplus_number >= token_cost:
                return True, ""
            else:
                return False, "合作商余额不足，请联系合作商充值"

        # 双重扣费：合作商和下级都要有足够条数
        partner_sufficient = partner_user.surplus_number >= token_cost
        user_sufficient = user.surplus_number >= token_cost
        
        if partner_sufficient and user_sufficient:
            return True, ""
        elif not partner_sufficient and not user_sufficient:
            return False, "您和合作商的余额都不足，请充值或联系合作商充值"
        elif not partner_sufficient:
            return False, "合作商余额不足，请联系合作商充值"
        else:  # not user_sufficient
            return False, "您的余额不足，请充值"

    # 普通用户：保持原有逻辑
    if user.surplus_number >= token_cost:
        return True, ""
    else:
        return False, "您的余额不足，请充值"


async def get_balance_owner(user: models.User) -> models.User:
    """根据计费策略确定实际扣费账户"""

    try:
        strategy = await _get_billing_strategy(user)
    except Exception:
        return user

    if (
        strategy.get("is_partner_subordinate")
        and strategy.get("billing_mode") == "partner_only"
        and strategy.get("partner_user") is not None
    ):
        return strategy["partner_user"]

    return user


async def get_balance_owner_id(user: models.User) -> int:
    """返回实际扣费账户ID"""

    owner = await get_balance_owner(user)
    return owner.pk


async def deduct_balance_by_tokens(
    user: models.User,
    tokens: int,
    usage_type: str,
    store: Optional[models.StoreInformation] = None,
    cost_multiplier: float = 1.0,
    log_prefix: str = "",
    details: str = ""
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    基于 token 数量扣费的统一工具函数（支持合作商双重扣费）

    这是项目中所有基于 token 扣费场景的统一入口，提供完整的扣费流程：
    检查余额 → 原子扣费 → 刷新余额 → 记录使用详情 → 统一日志

    支持三种扣费模式：
    1. normal: 普通用户扣费（原有逻辑）
    2. partner_only: 合作商下级用户有包月权益时，仅扣合作商
    3. dual: 合作商下级用户无包月权益时，双重扣费

    Args:
        user: 用户对象
        tokens: 本次消耗的 token 数量
        usage_type: 使用类型（"ai回复"、"图片识别"、"标题生成"、"商品参数提取"等）
        store: 店铺对象（可选，用于余额不足时的店铺关闭等操作）
        cost_multiplier: 费率倍数（默认1.0，意图识别为2.0）
        log_prefix: 日志前缀（如 "[AI回复]"、"[图片识别]"）
        details: 详细信息（可选，用于日志记录）

    Returns:
        Tuple[bool, Optional[Dict]]:
        - bool: True=扣费成功，False=余额不足
        - dict: 扣费详情（包含扣除的条数、剩余条数、token数等）
    """
    try:
        # 1. 计算扣费条数（应用倍数）
        base_cost = math.ceil(tokens / 1000) if tokens > 0 else 0
        token_cost = math.ceil(base_cost * cost_multiplier)

        if token_cost == 0:
            logger.warning(f"{log_prefix} tokens为0，跳过扣费")
            return True, {
                "status": "success",
                "deducted": 0,
                "tokens": 0,
                "remaining": user.surplus_number,
                "multiplier": cost_multiplier,
                "user": user.username
            }

        # 2. 获取计费策略
        billing_strategy = await _get_billing_strategy(user)

        # 3. 根据策略执行不同的扣费逻辑
        if billing_strategy["is_partner_subordinate"]:
            # 合作商下级用户：执行双重扣费逻辑
            success, result = await _execute_partner_billing(
                user=user,
                partner_user=billing_strategy["partner_user"],
                token_cost=token_cost,
                billing_mode=billing_strategy["billing_mode"],
                usage_type=usage_type,
                tokens=tokens,
                store=store,
            )

            if not success:
                # 余额不足处理
                await _handle_partner_insufficient_balance(
                    user, billing_strategy["partner_user"],
                    token_cost, usage_type, result
                )
                return False, result

            # 记录成功日志
            _log_partner_billing_success(log_prefix, user, billing_strategy, result, details)

            # 触发告警检查
            await _trigger_balance_alerts(user, billing_strategy["partner_user"])

            return True, result

        else:
            # 普通用户：保持原有逻辑完全不变
            return await _execute_normal_billing(
                user, tokens, token_cost, usage_type, store,
                cost_multiplier, log_prefix, details
            )

    except Exception as e:
        logger.error(
            f"{log_prefix} 扣费异常 - user: {user.username}, tokens: {tokens}, "
            f"错误: {str(e)}", exc_info=True
        )
        return False, {
            "status": "error",
            "message": f"扣费异常: {str(e)}",
            "user": user.username
        }



async def deduct_single_balance(
    user: models.User,
    usage_type: str,
    details: str = "",
    log_prefix: str = "",
    store: Optional[models.StoreInformation] = None,
) -> bool:
    """
    扣减1条余额的统一工具函数（支持合作商双重扣费策略）

    适用于固定扣费场景（如催单回复、特殊消息回复等）
    现在支持与 deduct_balance_by_tokens 相同的三种扣费模式：
    1. normal: 普通用户扣费
    2. partner_only: 合作商下级用户有包月权益时，仅扣合作商
    3. dual: 合作商下级用户无包月权益时，双重扣费

    Args:
        user: 用户对象
        usage_type: 使用类型（如"催单回复"、"特殊消息回复"等）
        details: 详细信息（可选，用于日志记录）
        log_prefix: 日志前缀（可选，如"[催单扣费]"）
        store: 店铺对象（可选，用于余额不足时的处理）

    Returns:
        bool: 扣费是否成功
    """
    try:
        # 直接复用 deduct_balance_by_tokens 的统一策略
        # 1000 tokens = 1条，cost_multiplier=1.0
        success, result = await deduct_balance_by_tokens(
            user=user,
            tokens=1000,  # 1000 tokens = 1条
            usage_type=usage_type,
            store=store,
            cost_multiplier=1.0,
            log_prefix=log_prefix,
            details=details
        )
        
        return success

    except Exception as e:
        # 记录失败日志
        error_msg = f"扣费失败: {e}, user: {user.username}, 类型: {usage_type}"
        if log_prefix:
            logger.error(f"{log_prefix} {error_msg}")
        else:
            logger.error(f"[余额扣费] {error_msg}")

        return False


async def increase_balance(
    user: models.User,
    amount: int,
    usage_type: str = "充值",
    log_prefix: str = "[余额充值]",
    details: str = ""
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    增加用户余额的统一工具函数（原子操作）

    适用于充值、赠送、补偿等增加余额的场景

    Args:
        user: 用户对象
        amount: 增加的条数
        usage_type: 类型（"充值"、"套餐赠送"、"系统补偿"等）
        log_prefix: 日志前缀
        details: 详细信息

    Returns:
        Tuple[bool, Dict]: 操作结果和详情
            成功时: (True, {
                "status": "success",
                "added": 增加的条数,
                "remaining": 剩余条数,
                "user": 用户名
            })
            失败时: (False, {
                "status": "error",
                "message": 错误信息,
                "user": 用户名
            })
    """
    try:
        if amount <= 0:
            logger.warning(f"{log_prefix} 充值金额必须大于0 - user: {user.username}, amount: {amount}")
            return False, {
                "status": "error",
                "message": "充值金额必须大于0",
                "amount": amount,
                "user": user.username
            }

        # 使用原子操作增加余额
        updated_count = await models.User.objects.filter(
            pk=user.pk
        ).aupdate(surplus_number=F('surplus_number') + amount)

        if updated_count == 0:
            # 用户不存在
            logger.error(f"{log_prefix} 用户不存在 - user: {user.username}")
            return False, {
                "status": "error",
                "message": "用户不存在",
                "user": user.username
            }

        # 刷新用户对象获取最新余额
        await user.arefresh_from_db(fields=['surplus_number'])

        # 记录充值详情
        await models.AIUsageDetails.objects.acreate(
            user=user,
            use_number=amount,
            surplus=user.surplus_number,
            use_type=usage_type,
            tokens=0,
            consumer=user,
        )

        # 输出日志
        log_msg = f"充值成功 - user: {user.username}, 充值: {amount}条, 剩余: {user.surplus_number}条"
        if details:
            log_msg += f", 详情: {details}"

        logger.info(f"{log_prefix} {log_msg}")

        return True, {
            "status": "success",
            "added": amount,
            "remaining": user.surplus_number,
            "user": user.username
        }

    except Exception as e:
        logger.error(
            f"{log_prefix} 充值异常 - user: {user.username}, amount: {amount}, "
            f"错误: {str(e)}", exc_info=True
        )
        return False, {
            "status": "error",
            "message": f"充值异常: {str(e)}",
            "user": user.username
        }


# ==================== 合作商双重扣费功能 ====================

async def _get_billing_strategy(user: models.User) -> Dict[str, Any]:
    """
    获取用户的计费策略

    Returns:
        {
            "is_partner_subordinate": bool,  # 是否为合作商下级用户
            "partner_user": User | None,     # 上级合作商用户
            "has_monthly_entitlement": bool, # 是否有有效包月权益
            "billing_mode": str              # "normal" | "partner_only" | "dual"
        }
    """
    from django.utils import timezone
    from datetime import timedelta

    strategy = {
        "is_partner_subordinate": False,
        "partner_user": None,
        "has_monthly_entitlement": False,
        "billing_mode": "normal"  # 默认普通扣费
    }

    # 1. 检查是否有上级用户且上级是合作商
    if user.superior_id:
        try:
            superior = await models.User.objects.select_related().aget(pk=user.superior_id)
            is_partner = await superior.groups.filter(name='合作商').aexists()

            if is_partner:
                strategy["is_partner_subordinate"] = True
                strategy["partner_user"] = superior

                # 2. 检查是否有有效包月权益（按30天有效期判断）
                now = timezone.now()

                has_entitlement = await models.PartnerMonthlyEntitlement.objects.filter(
                    user=user,
                    partner=superior,
                    is_active=True,
                    start_at__lte=now,    # 权益已开始
                    end_at__gt=now        # 权益未过期（30天内）
                ).aexists()

                strategy["has_monthly_entitlement"] = has_entitlement
                strategy["billing_mode"] = "partner_only" if has_entitlement else "dual"
        except models.User.DoesNotExist:
            pass

    return strategy


async def _execute_partner_billing(
    user: models.User,
    partner_user: models.User,
    token_cost: int,
    billing_mode: str,
    usage_type: str,
    tokens: int,
    store: Optional[models.StoreInformation] = None,
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    执行合作商相关的扣费逻辑

    Args:
        billing_mode: "partner_only" | "dual"
    """

    # 1. 余额检查
    if billing_mode == "partner_only":
        # 仅检查合作商余额
        if partner_user.surplus_number < token_cost:
            return False, {
                "status": "insufficient_balance",
                "message": "合作商余额不足",
                "required": token_cost,
                "partner_available": partner_user.surplus_number,
                "billing_mode": billing_mode
            }
    else:  # dual
        # 检查双方余额
        if partner_user.surplus_number < token_cost or user.surplus_number < token_cost:
            return False, {
                "status": "insufficient_balance",
                "message": "余额不足",
                "required": token_cost,
                "partner_available": partner_user.surplus_number,
                "user_available": user.surplus_number,
                "billing_mode": billing_mode
            }

    # 2. 原子扣费（带回滚）
    try:
        # 先扣合作商
        partner_updated = await models.User.objects.filter(
            pk=partner_user.pk,
            surplus_number__gte=token_cost
        ).aupdate(surplus_number=F('surplus_number') - token_cost)

        if partner_updated == 0:
            return False, {
                "status": "concurrent_conflict",
                "message": "合作商余额扣减失败（并发冲突）",
                "billing_mode": billing_mode
            }

        # 刷新合作商余额
        await partner_user.arefresh_from_db(fields=['surplus_number'])

        # 如果是双重扣费，再扣用户
        if billing_mode == "dual":
            user_updated = await models.User.objects.filter(
                pk=user.pk,
                surplus_number__gte=token_cost
            ).aupdate(surplus_number=F('surplus_number') - token_cost)

            if user_updated == 0:
                # 回滚合作商扣费
                await models.User.objects.filter(pk=partner_user.pk).aupdate(
                    surplus_number=F('surplus_number') + token_cost
                )
                return False, {
                    "status": "concurrent_conflict",
                    "message": "用户余额扣减失败，已回滚合作商扣费",
                    "billing_mode": billing_mode
                }

            # 刷新用户余额
            await user.arefresh_from_db(fields=['surplus_number'])

        # 3. 记录使用详情
        # 合作商扣费记录
        await models.AIUsageDetails.objects.acreate(
            use_number=token_cost,
            surplus=partner_user.surplus_number,
            use_type=usage_type,
            user=partner_user,  # 记录到合作商
            tokens=tokens,
            consumer=user,
            store=store if usage_type not in ["充值", "购买合作商套餐", "套餐赠送"] else None,
        )

        # 用户扣费记录（如果是双重扣费）
        if billing_mode == "dual":
            await models.AIUsageDetails.objects.acreate(
                use_number=token_cost,
                surplus=user.surplus_number,
                use_type=usage_type,
                user=user,
                tokens=tokens,
                consumer=user,
                store=store if usage_type not in ["充值", "购买合作商套餐", "套餐赠送"] else None,
            )

        return True, {
            "status": "success",
            "billing_mode": billing_mode,
            "partner_deducted": token_cost,
            "user_deducted": token_cost if billing_mode == "dual" else 0,
            "partner_remaining": partner_user.surplus_number,
            "user_remaining": user.surplus_number
        }

    except Exception as e:
        logger.error(f"合作商扣费异常: {e}")
        return False, {
            "status": "error",
            "message": f"扣费异常: {str(e)}",
            "billing_mode": billing_mode
        }


async def _handle_partner_insufficient_balance(
    user: models.User,
    partner_user: models.User,
    token_cost: int,
    usage_type: str,
    error_info: Dict
):
    """处理合作商相关的余额不足情况"""

    try:
        # 创建用户通知
        await models.SystemNotify.objects.acreate(
            title=_get_notification_title(usage_type),
            content="您的上级合作商余额不足，请联系合作商充值或购买套餐",
            notify_type="系统通知",
            user=user
        )

        # 创建合作商通知
        await models.SystemNotify.objects.acreate(
            title="下级用户余额不足",
            content=f"您的下级用户 {user.username} 因余额不足无法使用AI服务，请及时充值",
            notify_type="系统通知",
            user=partner_user
        )

        logger.info(f"已创建合作商余额不足通知 - user: {user.username}, partner: {partner_user.username}")

    except Exception as e:
        logger.error(f"创建合作商余额不足通知失败: {e}")


async def _trigger_balance_alerts(user: models.User, partner_user: models.User):
    """触发余额告警检查"""
    try:
        from utilis.alert_engine import alert_engine
        # 检查用户余额告警
        await alert_engine.check_and_trigger('balance', {
            'user': user,
            'value': user.surplus_number
        })
        # 检查合作商余额告警
        await alert_engine.check_and_trigger('balance', {
            'user': partner_user,
            'value': partner_user.surplus_number
        })
    except Exception:
        # 告警失败不影响主流程
        pass


def _log_partner_billing_success(log_prefix, user, strategy, result, details):
    """记录合作商扣费成功日志"""
    mode = result["billing_mode"]
    partner_name = strategy["partner_user"].username

    if mode == "partner_only":
        log_msg = (
            f"合作商包月扣费成功 - user: {user.username}, partner: {partner_name}, "
            f"合作商扣除: {result['partner_deducted']}条, 用户免扣费, "
            f"合作商剩余: {result['partner_remaining']}条"
        )
    else:
        log_msg = (
            f"双重扣费成功 - user: {user.username}, partner: {partner_name}, "
            f"各扣除: {result['user_deducted']}条, "
            f"用户剩余: {result['user_remaining']}条, 合作商剩余: {result['partner_remaining']}条"
        )

    if details:
        log_msg += f", 详情: {details}"

    logger.info(f"{log_prefix} {log_msg}")


async def _execute_normal_billing(
    user: models.User,
    tokens: int,
    token_cost: int,
    usage_type: str,
    store: Optional[models.StoreInformation],
    cost_multiplier: float,
    log_prefix: str,
    details: str
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    执行普通用户的扣费逻辑（保持原有逻辑不变）
    """
    # 2. 检查余额是否充足（如果不足，统一处理）
    is_sufficient, error_msg, detail_info = await check_and_handle_insufficient_balance(
        user, tokens, usage_type, store
    )

    if not is_sufficient:
        logger.warning(
            f"{log_prefix} 余额不足 - user: {user.username}, 需要: {token_cost}, "
            f"剩余: {user.surplus_number}, 详情: {detail_info.get('message', '未知')}"
        )
        return False, {
            "status": "insufficient_balance",
            "message": detail_info.get('message', '余额不足'),
            "required": token_cost,
            "available": user.surplus_number,
            "user": user.username,
            **detail_info
        }

    # 3. 使用原子操作扣减余额
    updated_count = await models.User.objects.filter(
        pk=user.pk,
        surplus_number__gte=token_cost
    ).aupdate(surplus_number=F('surplus_number') - token_cost)

    if updated_count == 0:
        # 并发场景下的兜底：扣费失败（余额已被其他操作消耗）
        logger.error(
            f"{log_prefix} 扣费失败（并发冲突） - user: {user.username}, "
            f"需要: {token_cost}, 当前余额: {user.surplus_number}"
        )
        return False, {
            "status": "concurrent_conflict",
            "message": "余额扣减失败（并发冲突）",
            "required": token_cost,
            "user": user.username
        }

    # 4. 刷新用户对象获取最新余额
    await user.arefresh_from_db(fields=['surplus_number'])

    # 5. 记录 AI 使用详情
    await models.AIUsageDetails.objects.acreate(
        use_number=token_cost,
        surplus=user.surplus_number,
        use_type=usage_type,
        user=user,
        tokens=tokens,
        consumer=user,
        store=store if usage_type not in ["充值", "购买合作商套餐", "套餐赠送"] else None,
    )

    # 6. 输出统一格式日志
    log_msg = (
        f"扣费成功 - user: {user.username}, tokens: {tokens}, "
        f"扣除: {token_cost}条"
    )
    if cost_multiplier != 1.0:
        log_msg += f" (倍率: {cost_multiplier}x)"
    log_msg += f", 剩余: {user.surplus_number}条"
    if details:
        log_msg += f", 详情: {details}"

    logger.info(f"{log_prefix} {log_msg}")

    # 6.5. 条数告警检查
    try:
        from utilis.alert_engine import alert_engine
        await alert_engine.check_and_trigger('balance', {
            'user': user,
            'value': user.surplus_number
        })
    except Exception:
        pass

    # 7. 返回扣费详情
    return True, {
        "status": "success",
        "deducted": token_cost,
        "tokens": tokens,
        "remaining": user.surplus_number,
        "multiplier": cost_multiplier,
        "user": user.username
    }

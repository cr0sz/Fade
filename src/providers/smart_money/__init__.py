from .funding_flow import FundingFlowProvider
from .options_flow import OptionsFlowProvider
from .sec_edgar import SECEdgarProvider
from .whale_alert import WhaleAlertProvider

SMART_MONEY_PROVIDERS: dict[str, type] = {
    "whale_alert":  WhaleAlertProvider,
    "sec_edgar":    SECEdgarProvider,
    "options_flow": OptionsFlowProvider,
    "funding_flow": FundingFlowProvider,
}

__all__ = ["SMART_MONEY_PROVIDERS", "WhaleAlertProvider", "SECEdgarProvider", "OptionsFlowProvider", "FundingFlowProvider"]

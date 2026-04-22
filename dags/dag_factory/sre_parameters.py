from enum import Enum


class SREParameter(Enum):
    OWNER = 'owner'
    CAPABILITY = 'capability'
    SEVERITY = 'severity'
    SUB_CAPABILITY = 'sub_capability'
    BUSINESS_IMPACT = 'business_impact'
    CUSTOMER_IMPACT = 'customer_impact'

from typing import Dict

def compute_kpis(plan_text: str, validation: Dict) -> Dict[str, float]:
    base_cost = 100.0
    risk = 0.2 if validation.get("passes") else 0.6
    rop = 1.0 if validation.get("passes") else 0.7
    return {
        "kpi_cost": base_cost * (1.0 if validation.get("passes") else 1.1),
        "kpi_risk": risk,
        "kpi_rop": rop,
        "constraint_violations": 0 if validation.get("passes") else validation.get("violations", 1),
    }

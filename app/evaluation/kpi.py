import re
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class RiskCategory(Enum):
    VIBRATION = "vibration"
    PRESSURE = "pressure"
    TORQUE = "torque"
    FORMATION = "formation"
    EQUIPMENT = "equipment"
    ENVIRONMENTAL = "environmental"

@dataclass
class DrillingParameters:
    wob: Optional[float] = None  # Weight on Bit
    rpm: Optional[float] = None  # Rotations per minute
    flow_rate: Optional[float] = None  # Mud flow rate
    mud_weight: Optional[float] = None
    torque: Optional[float] = None
    
@dataclass
class BHAComponent:
    component_type: str
    manufacturer: str
    part_number: str
    position: int
    operating_limits: Dict

@dataclass
class FormationData:
    name: str
    depth_start: float
    depth_end: float
    rock_strength: Optional[float] = None
    pore_pressure: Optional[float] = None
    formation_type: Optional[str] = None

class KPICalculator:
    def __init__(self, graph_client=None, historical_data_client=None):
        """
        Initialize KPI calculator with graph and historical data access
        
        Args:
            graph_client: Neo4j client for accessing knowledge graph
            historical_data_client: Client for accessing historical performance data
        """
        self.graph_client = graph_client
        self.historical_data_client = historical_data_client
        
        # Industry benchmarks (these should come from the knowledge graph)
        self.benchmarks = {
            "avg_cost_per_foot": 500.0,
            "target_rop": 50.0,  # feet per hour
            "max_acceptable_risk": 0.3,
            "target_npt_hours": 24.0
        }
    
    def compute_kpis(self, plan_text: str, validation: Dict, well_id: str = None) -> Dict[str, float]:
        """
        Compute comprehensive KPIs for a drilling plan
        
        Args:
            plan_text: The drilling plan text/JSON
            validation: Validation results from engineering models
            well_id: Well identifier for context retrieval
            
        Returns:
            Dictionary of KPI scores and components
        """
        try:
            # Parse the drilling plan
            plan_data = self._parse_plan(plan_text)
            
            # Get contextual data from knowledge graph
            context = self._get_plan_context(well_id, plan_data) if well_id else {}
            
            # Calculate individual KPI components
            cost_metrics = self._calculate_cost_kpis(plan_data, validation, context)
            risk_metrics = self._calculate_risk_kpis(plan_data, validation, context)
            performance_metrics = self._calculate_performance_kpis(plan_data, validation, context)
            safety_metrics = self._calculate_safety_kpis(plan_data, validation, context)
            
            # Calculate composite scores
            kpi_cost = self._normalize_cost_score(cost_metrics)
            kpi_risk = self._normalize_risk_score(risk_metrics)
            kpi_rop = self._normalize_performance_score(performance_metrics)
            kpi_safety = self._normalize_safety_score(safety_metrics)
            
            # Calculate overall score (weighted average)
            kpi_overall = self._calculate_weighted_score({
                "cost": kpi_cost,
                "risk": kpi_risk, 
                "performance": kpi_rop,
                "safety": kpi_safety
            })
            
            return {
                # Main KPIs
                "kpi_overall": kpi_overall,
                "kpi_cost": kpi_cost,
                "kpi_risk": kpi_risk,
                "kpi_rop": kpi_rop,
                "kpi_safety": kpi_safety,
                
                # Detailed components
                "cost_components": cost_metrics,
                "risk_components": risk_metrics,
                "performance_components": performance_metrics,
                "safety_components": safety_metrics,
                
                # Validation-based metrics
                "constraint_violations": validation.get("violations", 0),
                "validation_confidence": validation.get("confidence", 0.0),
                
                # Context-aware metrics
                "historical_comparison": context.get("historical_performance", 0.0),
                "formation_difficulty": context.get("formation_complexity", 0.0),
            }
            
        except Exception as e:
            logger.error(f"Error computing KPIs: {str(e)}")
            # Return fallback scores
            return self._get_fallback_kpis(validation)
    
    def _parse_plan(self, plan_text: str) -> Dict:
        """Parse drilling plan text to extract structured data"""
        plan_data = {
            "parameters": DrillingParameters(),
            "bha_components": [],
            "formations": [],
            "sections": []
        }
        
        try:
            # Try to parse as JSON first
            if plan_text.strip().startswith('{'):
                json_data = json.loads(plan_text)
                plan_data.update(json_data)
            else:
                # Parse text-based plan using regex patterns
                plan_data = self._parse_text_plan(plan_text)
                
        except json.JSONDecodeError:
            # Fallback to text parsing
            plan_data = self._parse_text_plan(plan_text)
            
        return plan_data
    
    def _parse_text_plan(self, plan_text: str) -> Dict:
        """Parse text-based drilling plan using regex patterns"""
        plan_data = {"parameters": {}, "bha_components": [], "sections": []}
        
        # Extract drilling parameters
        wob_match = re.search(r'WOB[:\s]+(\d+(?:\.\d+)?)', plan_text, re.IGNORECASE)
        rpm_match = re.search(r'RPM[:\s]+(\d+(?:\.\d+)?)', plan_text, re.IGNORECASE)
        flow_match = re.search(r'Flow[:\s]+(\d+(?:\.\d+)?)', plan_text, re.IGNORECASE)
        
        if wob_match:
            plan_data["parameters"]["wob"] = float(wob_match.group(1))
        if rpm_match:
            plan_data["parameters"]["rpm"] = float(rpm_match.group(1))
        if flow_match:
            plan_data["parameters"]["flow_rate"] = float(flow_match.group(1))
            
        # Extract BHA components (simplified)
        bha_pattern = r'(bit|motor|mwd|lwd|stabilizer)[:\s]+([^\n]+)'
        for match in re.finditer(bha_pattern, plan_text, re.IGNORECASE):
            component_type = match.group(1).lower()
            description = match.group(2).strip()
            plan_data["bha_components"].append({
                "type": component_type,
                "description": description
            })
        
        return plan_data
    
    def _get_plan_context(self, well_id: str, plan_data: Dict) -> Dict:
        """Retrieve contextual data from knowledge graph"""
        context = {}
        
        if self.graph_client:
            try:
                # Query for historical performance of similar plans
                context["historical_performance"] = self._query_historical_performance(well_id, plan_data)
                
                # Query for formation complexity
                context["formation_complexity"] = self._query_formation_complexity(well_id)
                
                # Query for offset well performance
                context["offset_performance"] = self._query_offset_wells(well_id)
                
            except Exception as e:
                logger.warning(f"Failed to retrieve graph context: {str(e)}")
                
        return context
    
    def _calculate_cost_kpis(self, plan_data: Dict, validation: Dict, context: Dict) -> Dict:
        """Calculate cost-related KPI components"""
        base_drilling_cost = 1000.0  # Base cost per day
        
        # BHA cost estimation
        bha_cost = self._estimate_bha_cost(plan_data.get("bha_components", []))
        
        # Time-based costs (from drilling parameters and formation data)
        estimated_time = self._estimate_drilling_time(plan_data, context)
        time_cost = estimated_time * base_drilling_cost
        
        # Mud and consumables cost
        consumables_cost = self._estimate_consumables_cost(plan_data)
        
        # Risk-adjusted costs (from validation failures)
        risk_cost_multiplier = 1.0 + (validation.get("violations", 0) * 0.15)
        
        total_cost = (bha_cost + time_cost + consumables_cost) * risk_cost_multiplier
        
        return {
            "bha_cost": bha_cost,
            "time_cost": time_cost,
            "consumables_cost": consumables_cost,
            "risk_multiplier": risk_cost_multiplier,
            "total_estimated_cost": total_cost,
            "cost_per_foot": total_cost / max(context.get("well_depth", 1000), 1000)
        }
    
    def _calculate_risk_kpis(self, plan_data: Dict, validation: Dict, context: Dict) -> Dict:
        """Calculate risk-related KPI components"""
        risk_components = {}
        
        # Validation-based risks
        validation_risk = min(validation.get("violations", 0) * 0.2, 1.0)
        
        # Parameter-based risks
        drilling_params = plan_data.get("parameters", {})
        
        # Vibration risk (based on RPM and formation type)
        vibration_risk = self._assess_vibration_risk(drilling_params, context)
        
        # Pressure risk (based on mud weight and formation pressure)
        pressure_risk = self._assess_pressure_risk(drilling_params, context)
        
        # Equipment risk (based on BHA configuration)
        equipment_risk = self._assess_equipment_risk(plan_data.get("bha_components", []))
        
        # Formation-specific risks
        formation_risk = context.get("formation_complexity", 0.3)
        
        risk_components = {
            "validation_risk": validation_risk,
            "vibration_risk": vibration_risk,
            "pressure_risk": pressure_risk,
            "equipment_risk": equipment_risk,
            "formation_risk": formation_risk,
            "overall_risk": sum([validation_risk, vibration_risk, pressure_risk, equipment_risk, formation_risk]) / 5
        }
        
        return risk_components
    
    def _calculate_performance_kpis(self, plan_data: Dict, validation: Dict, context: Dict) -> Dict:
        """Calculate performance-related KPI components"""
        drilling_params = plan_data.get("parameters", {})
        
        # Estimated ROP based on parameters and formation
        estimated_rop = self._estimate_rop(drilling_params, context)
        
        # Efficiency metrics
        mechanical_efficiency = self._calculate_mechanical_efficiency(drilling_params)
        hydraulic_efficiency = self._calculate_hydraulic_efficiency(drilling_params, validation)
        
        # Historical comparison
        historical_performance = context.get("historical_performance", 0.5)
        
        return {
            "estimated_rop": estimated_rop,
            "mechanical_efficiency": mechanical_efficiency,
            "hydraulic_efficiency": hydraulic_efficiency,
            "historical_comparison": historical_performance,
            "rop_normalized": min(estimated_rop / self.benchmarks["target_rop"], 2.0)
        }
    
    def _calculate_safety_kpis(self, plan_data: Dict, validation: Dict, context: Dict) -> Dict:
        """Calculate safety-related KPI components"""
        safety_score = 1.0
        
        # Reduce safety score based on validation violations
        critical_violations = validation.get("critical_violations", 0)
        safety_score -= critical_violations * 0.3
        
        # Environmental considerations
        environmental_risk = self._assess_environmental_risk(plan_data, context)
        
        # Well control considerations
        well_control_risk = self._assess_well_control_risk(plan_data, context)
        
        return {
            "safety_score": max(safety_score, 0.0),
            "environmental_risk": environmental_risk,
            "well_control_risk": well_control_risk,
            "critical_violations": critical_violations
        }
    
    # Helper methods for risk assessment
    def _assess_vibration_risk(self, params: Dict, context: Dict) -> float:
        """Assess vibration risk based on RPM and formation characteristics"""
        rpm = params.get("rpm", 120)
        formation_hardness = context.get("formation_hardness", 0.5)
        
        # Higher RPM in hard formations increases vibration risk
        if rpm > 150 and formation_hardness > 0.7:
            return 0.8
        elif rpm > 120 and formation_hardness > 0.5:
            return 0.5
        else:
            return 0.2
    
    def _assess_pressure_risk(self, params: Dict, context: Dict) -> float:
        """Assess pressure-related risks"""
        mud_weight = params.get("mud_weight", 9.0)
        pore_pressure = context.get("pore_pressure", 8.5)
        
        pressure_margin = mud_weight - pore_pressure
        if pressure_margin < 0.5:
            return 0.9  # High risk
        elif pressure_margin < 1.0:
            return 0.5  # Medium risk
        else:
            return 0.1  # Low risk
    
    def _assess_equipment_risk(self, bha_components: List[Dict]) -> float:
        """Assess equipment-related risks"""
        if not bha_components:
            return 0.5
        
        # Simple heuristic: more complex BHA = higher risk
        complexity_score = len(bha_components) / 10.0
        return min(complexity_score, 1.0)
    
    def _estimate_rop(self, params: Dict, context: Dict) -> float:
        """Estimate Rate of Penetration"""
        base_rop = 30.0  # feet per hour
        
        # Adjust based on formation hardness
        formation_factor = 1.0 - context.get("formation_hardness", 0.5) * 0.5
        
        # Adjust based on drilling parameters
        wob = params.get("wob", 30)
        rpm = params.get("rpm", 120)
        
        wob_factor = min(wob / 40.0, 1.5)  # Optimal around 40K lbs
        rpm_factor = min(rpm / 120.0, 1.3)  # Optimal around 120 RPM
        
        estimated_rop = base_rop * formation_factor * wob_factor * rpm_factor
        return max(estimated_rop, 5.0)  # Minimum 5 ft/hr
    
    def _normalize_cost_score(self, cost_metrics: Dict) -> float:
        """Normalize cost metrics to 0-1 scale (lower is better)"""
        cost_per_foot = cost_metrics.get("cost_per_foot", 500)
        benchmark = self.benchmarks["avg_cost_per_foot"]
        
        # Score decreases as cost increases above benchmark
        if cost_per_foot <= benchmark:
            return 1.0 - (cost_per_foot / benchmark) * 0.3
        else:
            return max(0.7 - ((cost_per_foot - benchmark) / benchmark), 0.0)
    
    def _normalize_risk_score(self, risk_metrics: Dict) -> float:
        """Normalize risk metrics to 0-1 scale (lower risk = higher score)"""
        overall_risk = risk_metrics.get("overall_risk", 0.5)
        return max(1.0 - overall_risk, 0.0)
    
    def _normalize_performance_score(self, performance_metrics: Dict) -> float:
        """Normalize performance metrics to 0-1 scale"""
        rop_normalized = performance_metrics.get("rop_normalized", 0.5)
        efficiency = (performance_metrics.get("mechanical_efficiency", 0.7) + 
                     performance_metrics.get("hydraulic_efficiency", 0.7)) / 2
        
        return (rop_normalized * 0.6 + efficiency * 0.4)
    
    def _normalize_safety_score(self, safety_metrics: Dict) -> float:
        """Normalize safety metrics to 0-1 scale"""
        return safety_metrics.get("safety_score", 0.8)
    
    def _calculate_weighted_score(self, scores: Dict) -> float:
        """Calculate weighted overall score"""
        weights = {
            "cost": 0.25,
            "risk": 0.35,
            "performance": 0.25,
            "safety": 0.15
        }
        
        return sum(scores[key] * weights[key] for key in weights)
    
    def _get_fallback_kpis(self, validation: Dict) -> Dict:
        """Return basic KPIs when detailed calculation fails"""
        passes = validation.get("passes", False)
        violations = validation.get("violations", 1 if not passes else 0)
        
        base_scores = {
            "kpi_overall": 0.7 if passes else 0.3,
            "kpi_cost": 0.8 if passes else 0.4,
            "kpi_risk": 0.8 if passes else 0.2,
            "kpi_rop": 1.0 if passes else 0.6,
            "kpi_safety": 0.9 if passes else 0.5,
            "constraint_violations": violations,
            "validation_confidence": validation.get("confidence", 0.5)
        }
        
        return base_scores
    
    # Placeholder methods for future integration with knowledge graph
    def _query_historical_performance(self, well_id: str, plan_data: Dict) -> float:
        """Query historical performance from knowledge graph"""
        # TODO: Implement graph query for similar historical plans
        return 0.6
    
    def _query_formation_complexity(self, well_id: str) -> float:
        """Query formation complexity from knowledge graph"""
        # TODO: Implement graph query for formation data
        return 0.4
    
    def _query_offset_wells(self, well_id: str) -> Dict:
        """Query offset well performance from knowledge graph"""
        # TODO: Implement graph query for offset well data
        return {"avg_rop": 35.0, "avg_cost": 450.0}
    
    def _estimate_bha_cost(self, components: List[Dict]) -> float:
        """Estimate BHA cost based on components"""
        # Simple estimation - should be replaced with actual catalog lookup
        base_cost_per_component = 50000
        return len(components) * base_cost_per_component
    
    def _estimate_drilling_time(self, plan_data: Dict, context: Dict) -> float:
        """Estimate total drilling time in hours"""
        estimated_rop = self._estimate_rop(plan_data.get("parameters", {}), context)
        well_depth = context.get("well_depth", 10000)
        return well_depth / max(estimated_rop, 1.0)
    
    def _estimate_consumables_cost(self, plan_data: Dict) -> float:
        """Estimate mud and consumables cost"""
        # Simplified calculation
        return 25000.0
    
    def _calculate_mechanical_efficiency(self, params: Dict) -> float:
        """Calculate mechanical drilling efficiency"""
        # Simplified heuristic based on WOB and RPM
        wob = params.get("wob", 30)
        rpm = params.get("rpm", 120)
        
        # Optimal ranges
        wob_efficiency = 1.0 - abs(wob - 35) / 50.0  # Optimal around 35K lbs
        rpm_efficiency = 1.0 - abs(rpm - 120) / 120.0  # Optimal around 120 RPM
        
        return max((wob_efficiency + rpm_efficiency) / 2, 0.0)
    
    def _calculate_hydraulic_efficiency(self, params: Dict, validation: Dict) -> float:
        """Calculate hydraulic efficiency"""
        # Based on flow rate and pressure predictions
        if validation.get("hydraulics_ok", True):
            return 0.8
        else:
            return 0.4
    
    def _assess_environmental_risk(self, plan_data: Dict, context: Dict) -> float:
        """Assess environmental risk factors"""
        # Simplified assessment
        return 0.2  # Low environmental risk
    
    def _assess_well_control_risk(self, plan_data: Dict, context: Dict) -> float:
        """Assess well control risk factors"""
        mud_weight = plan_data.get("parameters", {}).get("mud_weight", 9.0)
        pore_pressure = context.get("pore_pressure", 8.5)
        
        if mud_weight < pore_pressure:
            return 0.9  # High risk
        elif mud_weight - pore_pressure < 0.5:
            return 0.6  # Medium risk
        else:
            return 0.2  # Low risk


# Example usage and integration with the existing system
def enhanced_compute_kpis(plan_text: str, validation: Dict, well_id: str = None, 
                         graph_client=None, historical_client=None) -> Dict[str, float]:
    """
    Enhanced KPI computation function that replaces the original simple version
    """
    calculator = KPICalculator(graph_client, historical_client)
    return calculator.compute_kpis(plan_text, validation, well_id)


# For backward compatibility, maintain the original function signature
def compute_kpis(plan_text: str, validation: Dict) -> Dict[str, float]:
    base_cost = 100.0
    risk = 0.2 if validation.get("passes") else 0.6
    rop = 1.0 if validation.get("passes") else 0.7
    
    # Ensure violations is treated as a number for backward compatibility
    violations = validation.get("violations", 0)
    if isinstance(violations, list):
        violations = len(violations)
    
    return {
        "kpi_cost": base_cost * (1.0 if validation.get("passes") else 1.1),
        "kpi_risk": risk,
        "kpi_rop": rop,
        "constraint_violations": violations,
    }
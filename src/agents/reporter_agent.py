"""
Reporter Agent for the PyUCX-AI Multi-Agent Framework.

This agent generates comprehensive migration reports and summaries
of the entire Unity Catalog migration analysis and planning process.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from langchain_core.messages import HumanMessage, SystemMessage

from .base_agent import BaseAgent
from ..core.agent_state import AgentState

logger = logging.getLogger(__name__)


class ReporterAgent(BaseAgent):
    """Agent responsible for generating final migration reports."""

    def execute(self, state: AgentState) -> AgentState:
        """Generate comprehensive migration report."""

        try:
            self._increment_iteration(state)
            self._add_message(state, "Generating migration report")

            # Get current notebook context
            current_notebook = state.get("current_notebook")
            if not current_notebook:
                return self._handle_error(state, "No current notebook for reporting")

            # Generate report for current notebook
            notebook_report = self._generate_notebook_report(state, current_notebook)

            # Check if this is the last notebook
            if self._is_final_notebook(state):
                # Generate comprehensive final report
                final_report = self._generate_final_report(state)
                state["final_report"] = final_report
                state["success"] = True
                state["should_continue"] = False

                self._add_message(
                    state, 
                    f"Migration analysis completed successfully for all {len(state['notebooks'])} notebooks"
                )
            else:
                self._add_message(
                    state, 
                    f"Report generated for {current_notebook['filename']}"
                )

            return state

        except Exception as e:
            return self._handle_error(state, f"Report generation failed: {str(e)}")

    def _is_final_notebook(self, state: AgentState) -> bool:
        """Check if we're processing the final notebook."""

        total_notebooks = len(state["notebooks"])
        
        # Count unique notebooks that have been converted (fully processed)
        converted_notebooks = set()
        for converted in state.get("converted_notebooks", []):
            converted_notebooks.add(converted.get("original_path"))
        
        completed_conversions = len(converted_notebooks)

        return completed_conversions >= total_notebooks

    def _generate_notebook_report(
        self, 
        state: AgentState, 
        notebook: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate a report for a single notebook."""

        notebook_path = notebook["path"]

        # Get related data for this notebook
        analysis = self._get_notebook_analysis(state, notebook_path)
        plan = self._get_notebook_plan(state, notebook_path)
        modifications = self._get_notebook_modifications(state, notebook_path)
        validations = self._get_notebook_validations(state, notebook_path)

        report = {
            "notebook_info": {
                "filename": notebook["filename"],
                "path": notebook_path,
                "cells": notebook.get("cell_count", 0),
                "code_cells": notebook.get("code_cell_count", 0)
            },
            "analysis_summary": self._summarize_analysis(analysis),
            "migration_plan_summary": self._summarize_plan(plan),
            "modifications_summary": self._summarize_modifications(modifications),
            "validation_summary": self._summarize_validations(validations),
            "recommendations": self._generate_notebook_recommendations(
                analysis, plan, modifications, validations
            ),
            "timestamp": datetime.now().isoformat()
        }

        return report

    def _get_notebook_analysis(self, state: AgentState, notebook_path: str) -> Optional[Dict[str, Any]]:
        """Get analysis results for a specific notebook."""

        for analysis in state.get("analysis_results", []):
            if analysis.get("notebook_path") == notebook_path:
                return analysis
        return None

    def _get_notebook_plan(self, state: AgentState, notebook_path: str) -> Optional[Dict[str, Any]]:
        """Get migration plan for a specific notebook."""

        for plan in state.get("migration_plans", []):
            if plan.get("notebook_path") == notebook_path:
                return plan
        return None

    def _get_notebook_modifications(self, state: AgentState, notebook_path: str) -> List[Dict[str, Any]]:
        """Get code modifications for a specific notebook."""

        return [
            mod for mod in state.get("code_modifications", [])
            if mod.get("file_path") == notebook_path
        ]

    def _get_notebook_validations(self, state: AgentState, notebook_path: str) -> List[Dict[str, Any]]:
        """Get validation results for a specific notebook."""

        return [
            val for val in state.get("validation_results", [])
            if val.get("file_path") == notebook_path
        ]

    def _summarize_analysis(self, analysis: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize analysis results."""

        if not analysis:
            return {"status": "not_completed"}

        return {
            "status": "completed",
            "complexity": analysis.get("migration_complexity", "unknown"),
            "issues_found": analysis.get("issues_found", 0),
            "estimated_effort_hours": analysis.get("estimated_effort_hours", 0),
            "issues_by_severity": analysis.get("issues_by_severity", {}),
            "issues_by_category": analysis.get("issues_by_category", {})
        }

    def _summarize_plan(self, plan: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize migration plan."""

        if not plan:
            return {"status": "not_completed"}

        return {
            "status": "completed",
            "priority": plan.get("priority", "unknown"),
            "estimated_duration_days": plan.get("estimated_duration_days", 0),
            "preparation_steps": len(plan.get("preparation_steps", [])),
            "code_changes": len(plan.get("code_changes", [])),
            "testing_steps": len(plan.get("testing_steps", [])),
            "deployment_steps": len(plan.get("deployment_steps", [])),
            "risks_identified": len(plan.get("risks", []))
        }

    def _summarize_modifications(self, modifications: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize code modifications."""

        if not modifications:
            return {"status": "no_modifications_needed"}

        change_types = {}
        confidence_levels = []

        for mod in modifications:
            change_type = mod.get("change_type", "unknown")
            change_types[change_type] = change_types.get(change_type, 0) + 1

            confidence = mod.get("confidence_level", 0)
            confidence_levels.append(confidence)

        avg_confidence = sum(confidence_levels) / len(confidence_levels) if confidence_levels else 0

        return {
            "status": "completed",
            "total_modifications": len(modifications),
            "change_types": change_types,
            "average_confidence": round(avg_confidence, 2),
            "high_confidence_changes": len([c for c in confidence_levels if c >= 0.8])
        }

    def _summarize_validations(self, validations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize validation results."""

        if not validations:
            return {"status": "not_completed"}

        total_validations = len(validations)
        passed_validations = len([v for v in validations if v.get("is_valid", False)])

        validation_types = [v.get("validation_type", "unknown") for v in validations]

        total_issues = sum(len(v.get("issues_found", [])) for v in validations)
        total_warnings = sum(len(v.get("warnings", [])) for v in validations)

        return {
            "status": "completed",
            "total_validations": total_validations,
            "passed_validations": passed_validations,
            "validation_types": validation_types,
            "total_issues": total_issues,
            "total_warnings": total_warnings,
            "overall_status": "passed" if passed_validations == total_validations else "failed"
        }

    def _generate_notebook_recommendations(
        self,
        analysis: Optional[Dict[str, Any]],
        plan: Optional[Dict[str, Any]], 
        modifications: List[Dict[str, Any]],
        validations: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate specific recommendations for the notebook."""

        recommendations = []

        # Analysis-based recommendations
        if analysis:
            complexity = analysis.get("migration_complexity", "medium")
            issues_count = analysis.get("issues_found", 0)

            if complexity == "high":
                recommendations.append("High complexity migration - consider breaking into phases")

            if issues_count > 10:
                recommendations.append("Large number of issues - prioritize critical errors first")

        # Plan-based recommendations
        if plan:
            priority = plan.get("priority", "medium")
            if priority == "high":
                recommendations.append("High priority migration - allocate experienced resources")

        # Modification-based recommendations
        if modifications:
            low_confidence_mods = [
                m for m in modifications 
                if m.get("confidence_level", 1.0) < 0.7
            ]

            if low_confidence_mods:
                recommendations.append("Some modifications have low confidence - require manual review")

        # Validation-based recommendations
        failed_validations = [v for v in validations if not v.get("is_valid", True)]
        if failed_validations:
            recommendations.append("Validation failures detected - address issues before migration")

        # Default recommendations
        if not recommendations:
            recommendations = [
                "Migration appears straightforward - proceed with standard testing",
                "Validate in development environment before production deployment"
            ]

        return recommendations

    def _generate_final_report(self, state: AgentState) -> Dict[str, Any]:
        """Generate comprehensive final report for all notebooks."""

        notebooks = state["notebooks"]
        analysis_results = state.get("analysis_results", [])
        migration_plans = state.get("migration_plans", [])
        code_modifications = state.get("code_modifications", [])
        validation_results = state.get("validation_results", [])

        # Overall statistics
        stats = self._calculate_overall_statistics(
            notebooks, analysis_results, migration_plans, 
            code_modifications, validation_results
        )

        # Priority analysis
        priority_breakdown = self._analyze_migration_priorities(migration_plans)

        # Risk assessment
        risk_assessment = self._assess_overall_risks(migration_plans, validation_results)

        # Timeline estimation
        timeline = self._estimate_overall_timeline(migration_plans)

        # Generate executive summary using LLM
        executive_summary = self._generate_executive_summary(stats, priority_breakdown, timeline)

        final_report = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "framework_version": "1.0.0",
                "total_notebooks": len(notebooks),
                "analysis_completion": "100%"
            },
            "executive_summary": executive_summary,
            "overall_statistics": stats,
            "priority_breakdown": priority_breakdown,
            "risk_assessment": risk_assessment,
            "timeline_estimation": timeline,
            "detailed_results": {
                "notebooks": [n["filename"] for n in notebooks],
                "analysis_results": analysis_results,
                "migration_plans": migration_plans,
                "code_modifications": code_modifications,
                "validation_results": validation_results
            },
            "recommendations": self._generate_final_recommendations(
                stats, priority_breakdown, risk_assessment
            )
        }

        return final_report

    def _calculate_overall_statistics(
        self,
        notebooks: List[Dict[str, Any]],
        analysis_results: List[Dict[str, Any]],
        migration_plans: List[Dict[str, Any]],
        code_modifications: List[Dict[str, Any]],
        validation_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate overall migration statistics."""

        total_notebooks = len(notebooks)
        total_code_cells = sum(nb.get("code_cell_count", 0) for nb in notebooks)

        # Issue statistics
        total_issues = sum(ar.get("issues_found", 0) for ar in analysis_results)

        # Complexity distribution
        complexity_counts = {}
        for ar in analysis_results:
            complexity = ar.get("migration_complexity", "unknown")
            complexity_counts[complexity] = complexity_counts.get(complexity, 0) + 1

        # Effort estimation
        total_effort_hours = sum(ar.get("estimated_effort_hours", 0) for ar in analysis_results)

        # Modification statistics
        total_modifications = len(code_modifications)

        # Validation statistics
        total_validations = len(validation_results)
        passed_validations = len([vr for vr in validation_results if vr.get("is_valid", False)])

        return {
            "notebooks": {
                "total": total_notebooks,
                "total_code_cells": total_code_cells
            },
            "issues": {
                "total_issues": total_issues,
                "average_per_notebook": round(total_issues / max(total_notebooks, 1), 1)
            },
            "complexity": {
                "distribution": complexity_counts,
                "total_effort_hours": total_effort_hours
            },
            "modifications": {
                "total_modifications": total_modifications,
                "average_per_notebook": round(total_modifications / max(total_notebooks, 1), 1)
            },
            "validations": {
                "total_validations": total_validations,
                "passed_validations": passed_validations,
                "success_rate": round((passed_validations / max(total_validations, 1)) * 100, 1)
            }
        }

    def _analyze_migration_priorities(self, migration_plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze migration priorities across notebooks."""

        priority_counts = {}

        for plan in migration_plans:
            priority = plan.get("priority", "medium")
            priority_counts[priority] = priority_counts.get(priority, 0) + 1

        return {
            "priority_distribution": priority_counts,
            "high_priority_notebooks": [
                plan.get("notebook_path", "unknown") 
                for plan in migration_plans 
                if plan.get("priority") == "high"
            ]
        }

    def _assess_overall_risks(
        self, 
        migration_plans: List[Dict[str, Any]], 
        validation_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Assess overall migration risks."""

        # Count validation failures
        validation_failures = len([vr for vr in validation_results if not vr.get("is_valid", True)])

        # Count high-risk migrations
        high_risk_count = len([
            plan for plan in migration_plans 
            if plan.get("priority") == "high" or len(plan.get("risks", [])) > 3
        ])

        # Overall risk level
        total_plans = len(migration_plans)
        risk_percentage = (high_risk_count / max(total_plans, 1)) * 100

        if risk_percentage > 50:
            risk_level = "high"
        elif risk_percentage > 25:
            risk_level = "medium"
        else:
            risk_level = "low"

        return {
            "overall_risk_level": risk_level,
            "high_risk_notebooks": high_risk_count,
            "validation_failures": validation_failures,
            "risk_percentage": round(risk_percentage, 1)
        }

    def _estimate_overall_timeline(self, migration_plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Estimate overall migration timeline."""

        total_days = sum(plan.get("estimated_duration_days", 1) for plan in migration_plans)

        # Assume some parallelization possible
        parallel_factor = 0.7  # 30% reduction due to parallelization
        estimated_days = max(1, int(total_days * parallel_factor))

        return {
            "total_effort_days": total_days,
            "estimated_calendar_days": estimated_days,
            "parallelization_factor": parallel_factor,
            "recommended_timeline": f"{estimated_days} days with parallel execution"
        }

    def _generate_executive_summary(
        self, 
        stats: Dict[str, Any], 
        priorities: Dict[str, Any], 
        timeline: Dict[str, Any]
    ) -> str:
        """Generate executive summary using LLM."""

        try:
            prompt = f"""
            Generate an executive summary for Unity Catalog migration analysis:

            Project Scope:
            - Total Notebooks: {stats['notebooks']['total']}
            - Total Code Cells: {stats['notebooks']['total_code_cells']}
            - Total Issues Found: {stats['issues']['total_issues']}

            Complexity Analysis:
            - Complexity Distribution: {stats['complexity']['distribution']}
            - Total Effort: {stats['complexity']['total_effort_hours']} hours

            Priority Breakdown:
            - Priority Distribution: {priorities['priority_distribution']}
            - High Priority Count: {len(priorities.get('high_priority_notebooks', []))}

            Timeline Estimate:
            - Total Effort: {timeline['total_effort_days']} days
            - Recommended Timeline: {timeline['recommended_timeline']}

            Provide a clear, concise executive summary highlighting key findings and recommendations.
            """

            messages = [
                SystemMessage(content="You are a Unity Catalog migration expert. Provide clear executive summaries for technical leadership."),
                HumanMessage(content=prompt)
            ]

            response = self.llm.invoke(messages)
            return response.content

        except Exception as e:
            logger.error(f"Failed to generate executive summary: {e}")
            return f"""
            Unity Catalog Migration Analysis Summary:

            Analyzed {stats['notebooks']['total']} notebooks with {stats['issues']['total_issues']} 
            total compatibility issues identified. Migration complexity varies with 
            {stats['complexity']['total_effort_hours']} total hours estimated for completion.

            Recommended approach: Address high-priority notebooks first, with parallel 
            execution reducing timeline to approximately {timeline['estimated_calendar_days']} days.
            """

    def _generate_final_recommendations(
        self,
        stats: Dict[str, Any],
        priorities: Dict[str, Any], 
        risks: Dict[str, Any]
    ) -> List[str]:
        """Generate final recommendations."""

        recommendations = []

        # Priority-based recommendations
        high_priority_count = len(priorities.get("high_priority_notebooks", []))
        if high_priority_count > 0:
            recommendations.append(f"Prioritize {high_priority_count} high-priority notebooks for immediate migration")

        # Risk-based recommendations
        if risks["overall_risk_level"] == "high":
            recommendations.append("High risk level detected - implement comprehensive testing strategy")
        elif risks["overall_risk_level"] == "medium":
            recommendations.append("Medium risk level - standard testing and validation procedures recommended")

        # Validation-based recommendations
        if risks["validation_failures"] > 0:
            recommendations.append(f"Address {risks['validation_failures']} validation failures before proceeding")

        # General recommendations
        recommendations.extend([
            "Set up Unity Catalog test environment before migration",
            "Train development team on Unity Catalog best practices", 
            "Implement rollback procedures for production migrations",
            "Monitor performance metrics post-migration"
        ])

        return recommendations

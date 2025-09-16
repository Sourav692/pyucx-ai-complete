"""
Base agent class for the PyUCX-AI Multi-Agent Framework.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from langchain_core.messages import HumanMessage, SystemMessage

from ..core.agent_state import AgentState

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """Base class for all agents in the framework."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the agent with configuration."""
        self.config = config
        self.llm = self._setup_llm()
        self.agent_name = self.__class__.__name__.replace("Agent", "").lower()

    def _setup_llm(self):
        """Setup the language model."""
        try:
            # Import ChatOpenAI and handle Pydantic compatibility
            from langchain_openai import ChatOpenAI
            
            # Try to fix Pydantic compatibility issues
            try:
                from langchain_core.callbacks import Callbacks
                from langchain_core.caches import BaseCache
                ChatOpenAI.model_rebuild()
            except (ImportError, Exception):
                # Fallback - proceed without model rebuild
                pass
            
            return ChatOpenAI(
                model=self.config.get("openai_model", "gpt-4o-mini"),
                temperature=self.config.get("temperature", 0.1),
                api_key=self.config.get("openai_api_key")
            )
        except Exception as e:
            logger.error(f"Failed to setup ChatOpenAI: {e}")
            raise

    @abstractmethod
    def execute(self, state: AgentState) -> AgentState:
        """Execute the agent's main functionality."""
        pass

    def _add_message(self, state: AgentState, message: str, message_type: str = "info"):
        """Add a message to the agent messages."""
        agent_message = {
            "agent": self.agent_name,
            "message": message,
            "type": message_type,
            "timestamp": str(logger.handlers[0].formatter.formatTime(logger.handlers[0], logger.makeRecord("", 0, "", 0, "", (), None)) if logger.handlers else "")
        }
        state["agent_messages"].append(agent_message)
        return state

    def _increment_iteration(self, state: AgentState) -> AgentState:
        """Increment the iteration counter."""
        state["iteration_count"] += 1
        return state

    def _handle_error(self, state: AgentState, error: str) -> AgentState:
        """Handle and record an error."""
        logger.error(f"{self.agent_name}: {error}")
        state["errors"].append(f"{self.agent_name}: {error}")
        return self._add_message(state, error, "error")

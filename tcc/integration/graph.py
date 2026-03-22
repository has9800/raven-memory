from __future__ import annotations

from typing import Annotated, Any, Optional

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from typing_extensions import TypedDict

from langgraph.graph.message import add_messages
from langgraph.types import interrupt

from tcc.integration.interceptor import TCCInterceptor
from tcc.integration.tools import TOOL_MAP


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    tcc_context: str
    session_id: str
    pending_approval: Optional[dict]


def _coerce_message(msg: Any) -> BaseMessage:
    if isinstance(msg, BaseMessage):
        return msg
    if isinstance(msg, dict):
        role = msg.get("role")
        content = msg.get("content", "")
        if role == "user":
            return HumanMessage(content=content)
        if role == "system":
            return SystemMessage(content=content)
        if role == "tool":
            return ToolMessage(content=content, tool_call_id=msg.get("tool_call_id", "tool"))
        if role == "assistant":
            return AIMessage(content=content, tool_calls=msg.get("tool_calls", []))
    return HumanMessage(content=str(msg))


def agent_node(state: AgentState, model, tools):
    system = f"""You are a lab assistant with persistent memory and access to tools.

## Project memory (from TCC):
{state['tcc_context']}

## Tools available:
- run_simulation(sim_name, duration_seconds): run a named simulation
- set_lights(state): turn lab lights on or off
- read_file(path): read a file from the workspace
- write_note(text): write a note to the project log

## To use a tool respond with EXACTLY this format:
TOOL: tool_name
ARGS: {{"param": "value"}}

Use tools when the request requires action. Answer directly from memory otherwise.
"""
    messages = [SystemMessage(content=system)] + [_coerce_message(m) for m in state["messages"]]
    response = model.invoke(messages)
    return {"messages": [response]}


def tools_node(state: AgentState, interceptor: TCCInterceptor, tool_map: dict):
    last_message = _coerce_message(state["messages"][-1])
    tool_calls = getattr(last_message, "tool_calls", None) or []
    results = []
    for tool_call in tool_calls:
        tool = tool_map[tool_call["name"]]
        result = tool.invoke(tool_call.get("args", {}))
        interceptor.record_tool_call(
            tool_call["name"],
            tool_call.get("args", {}),
            result,
            plan=f"executing {tool_call['name']}",
        )
        results.append(ToolMessage(content=str(result), tool_call_id=tool_call["id"]))
    return {"messages": results}


def interrupt_node(state: AgentState):
    last = _coerce_message(state["messages"][-1])
    tool_calls = getattr(last, "tool_calls", None) or []
    sim_calls = [tc for tc in tool_calls if tc["name"] == "run_simulation"]
    if sim_calls:
        approval = interrupt(
            {
                "message": f"Agent wants to run simulation: {sim_calls[0]['args']}",
                "action": "approve or modify params",
            }
        )
        if approval.get("approved") is False:
            return {"messages": [HumanMessage(content="Simulation cancelled by user.")]}
    return {}


def should_interrupt(state: AgentState) -> str:
    last = _coerce_message(state["messages"][-1])
    tool_calls = getattr(last, "tool_calls", None) or []
    if tool_calls:
        sim_calls = [tc for tc in tool_calls if tc["name"] == "run_simulation"]
        if sim_calls:
            return "interrupt"
        return "tools"
    return "end"


def after_tools(state: AgentState) -> str:
    last = _coerce_message(state["messages"][-1])
    tool_calls = getattr(last, "tool_calls", None) or []
    if tool_calls:
        return "agent"
    return "end"


def build_graph(model, tools, interceptor, checkpointer):
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(AgentState)

    graph.add_node("agent", lambda s: agent_node(s, model, tools))
    graph.add_node("tools", lambda s: tools_node(s, interceptor, TOOL_MAP))
    graph.add_node("interrupt", interrupt_node)

    graph.add_edge(START, "agent")
    graph.add_conditional_edges(
        "agent",
        should_interrupt,
        {"interrupt": "interrupt", "tools": "tools", "end": END},
    )
    graph.add_edge("interrupt", "tools")
    graph.add_conditional_edges("tools", after_tools, {"agent": "agent", "end": END})

    return graph.compile(checkpointer=checkpointer, interrupt_before=["interrupt"])

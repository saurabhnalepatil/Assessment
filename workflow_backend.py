import asyncio
import json
import time
from typing import Dict, List, Optional, Set, Callable
from enum import Enum
from dataclasses import dataclass, asdict
from collections import defaultdict
import pickle
from pathlib import Path


class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class Task:
    id: str
    dependencies: List[str]
    runtime: float
    dynamic: bool
    state: TaskState = TaskState.PENDING
    result: Optional[dict] = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class WorkflowExecution:
    workflow_id: str
    version: str
    name: str
    execution_id: str
    tasks: Dict[str, Task]
    start_time: float
    end_time: Optional[float] = None
    state: str = "RUNNING"


class WorkflowEngine:
    def __init__(self, state_dir: str = "./workflow_state"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(exist_ok=True)
        self.executions: Dict[str, WorkflowExecution] = {}
        self.task_handlers: Dict[str, Callable] = {}
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        
    def register_task_handler(self, task_id: str, handler: Callable):
        """Register custom handlers for specific task types"""
        self.task_handlers[task_id] = handler
    
    async def submit_workflow(self, workflow_def: dict) -> str:
        """Submit a workflow for execution"""
        execution_id = f"{workflow_def['name']}_{workflow_def['version']}_{int(time.time() * 1000)}"
        
       
        tasks = {}
        for task_def in workflow_def["tasks"]:
            task = Task(
                id=task_def["id"],
                dependencies=task_def["dependencies"],
                runtime=task_def["runtime"],
                dynamic=task_def["dynamic"]
            )
            tasks[task.id] = task
        
        # Create execution
        execution = WorkflowExecution(
            workflow_id=workflow_def["name"],
            version=workflow_def["version"],
            name=workflow_def["name"],
            execution_id=execution_id,
            tasks=tasks,
            start_time=time.time()
        )
        
        async with self._lock:
            self.executions[execution_id] = execution
            await self._persist_state(execution_id)
        
        asyncio.create_task(self._execute_workflow(execution_id))
        
        return execution_id
    
    async def _execute_workflow(self, execution_id: str):
        """Main workflow execution loop"""
        try:
            execution = self.executions[execution_id]
            print(f"[{execution_id}] Starting workflow execution (version: {execution.version})")
            
            while True:
                ready_tasks = self._get_ready_tasks(execution)
                
                if not ready_tasks:
                    if self._is_workflow_complete(execution):
                        execution.state = "SUCCESS"
                        execution.end_time = time.time()
                        await self._persist_state(execution_id)
                        print(f"[{execution_id}] Workflow completed successfully")
                        break
                    
                  
                    await asyncio.sleep(0.1)
                    continue
                
               
                task_coroutines = [
                    self._execute_task(execution_id, task_id)
                    for task_id in ready_tasks
                ]
                await asyncio.gather(*task_coroutines, return_exceptions=True)
                
        except Exception as e:
            execution = self.executions[execution_id]
            execution.state = "FAILED"
            execution.end_time = time.time()
            await self._persist_state(execution_id)
            print(f"[{execution_id}] Workflow failed: {str(e)}")
    
    
    
    def _get_ready_tasks(self, execution: WorkflowExecution) -> List[str]:
        """Get tasks that are ready to execute"""
        ready = []
        for task_id, task in execution.tasks.items():
            if task.state != TaskState.PENDING:
                continue
            
            # Check if all dependencies are satisfied
            deps_satisfied = all(
                execution.tasks.get(dep_id, Task(id="", dependencies=[], runtime=0, dynamic=False)).state == TaskState.SUCCESS
                for dep_id in task.dependencies
            )
            
            if deps_satisfied:
                ready.append(task_id)
        
        return ready
    
    
    
    def _is_workflow_complete(self, execution: WorkflowExecution) -> bool:
        """Check if all tasks are in terminal state"""
        for task in execution.tasks.values():
            if task.state in [TaskState.PENDING, TaskState.RUNNING]:
                return False
        return True
    
    
    
    async def _execute_task(self, execution_id: str, task_id: str):
        """Execute a single task"""
        execution = self.executions[execution_id]
        task = execution.tasks[task_id]
        
        try:
            # Update state to RUNNING
            task.state = TaskState.RUNNING
            task.start_time = time.time()
            await self._persist_state(execution_id)
            
            print(f"[{execution_id}] Task {task_id} started (runtime: {task.runtime}s)")
            
            # Execute task (simulate with sleep or call custom handler)
            if task_id in self.task_handlers:
                result = await self.task_handlers[task_id](execution, task)
            else:
                await asyncio.sleep(task.runtime)
                result = {"status": "completed", "task_id": task_id}
            
            task.result = result
            
            # Handle dynamic task generation
            if task.dynamic:
                await self._handle_dynamic_tasks(execution_id, task_id, result)
            
            # Update state to SUCCESS
            task.state = TaskState.SUCCESS
            task.end_time = time.time()
            await self._persist_state(execution_id)
            
            print(f"[{execution_id}] Task {task_id} completed successfully")
            
        except Exception as e:
            task.state = TaskState.FAILED
            task.error = str(e)
            task.end_time = time.time()
            await self._persist_state(execution_id)
            print(f"[{execution_id}] Task {task_id} failed: {str(e)}")
    
    
    
    async def _handle_dynamic_tasks(self, execution_id: str, parent_task_id: str, result: dict):
        """Handle dynamic task generation"""
        # Check if result contains new tasks
        if "dynamic_tasks" in result:
            async with self._lock:
                execution = self.executions[execution_id]
                new_tasks = result["dynamic_tasks"]
                
                print(f"[{execution_id}] Task {parent_task_id} dynamically generated {len(new_tasks)} tasks")
                
                for task_def in new_tasks:
                    task = Task(
                        id=task_def["id"],
                        dependencies=task_def["dependencies"],
                        runtime=task_def["runtime"],
                        dynamic=task_def.get("dynamic", False)
                    )
                    execution.tasks[task.id] = task
                
                await self._persist_state(execution_id)
   
   
   
    
    async def _persist_state(self, execution_id: str):
        """Persist execution state to disk"""
        execution = self.executions[execution_id]
        state_file = self.state_dir / f"{execution_id}.pkl"
        
        # Convert to serializable format
        state_data = {
            "execution": {
                "workflow_id": execution.workflow_id,
                "version": execution.version,
                "name": execution.name,
                "execution_id": execution.execution_id,
                "start_time": execution.start_time,
                "end_time": execution.end_time,
                "state": execution.state
            },
            "tasks": {
                task_id: {
                    "id": task.id,
                    "dependencies": task.dependencies,
                    "runtime": task.runtime,
                    "dynamic": task.dynamic,
                    "state": task.state.value,
                    "result": task.result,
                    "error": task.error,
                    "start_time": task.start_time,
                    "end_time": task.end_time
                }
                for task_id, task in execution.tasks.items()
            }
        }
        
        with open(state_file, "wb") as f:
            pickle.dump(state_data, f)
    
    async def recover_execution(self, execution_id: str) -> bool:
        """Recover an execution from persisted state"""
        state_file = self.state_dir / f"{execution_id}.pkl"
        
        if not state_file.exists():
            print(f"No state file found for execution: {execution_id}")
            return False
        
        try:
            with open(state_file, "rb") as f:
                state_data = pickle.load(f)
            
            # Reconstruct execution
            exec_data = state_data["execution"]
            tasks = {}
            
            for task_id, task_data in state_data["tasks"].items():
                task = Task(
                    id=task_data["id"],
                    dependencies=task_data["dependencies"],
                    runtime=task_data["runtime"],
                    dynamic=task_data["dynamic"],
                    state=TaskState(task_data["state"]),
                    result=task_data["result"],
                    error=task_data["error"],
                    start_time=task_data["start_time"],
                    end_time=task_data["end_time"]
                )
                tasks[task_id] = task
            
            execution = WorkflowExecution(
                workflow_id=exec_data["workflow_id"],
                version=exec_data["version"],
                name=exec_data["name"],
                execution_id=exec_data["execution_id"],
                tasks=tasks,
                start_time=exec_data["start_time"],
                end_time=exec_data["end_time"],
                state=exec_data["state"]
            )
            
            async with self._lock:
                self.executions[execution_id] = execution
            
            print(f"Recovered execution: {execution_id}")
            
            # Reset RUNNING tasks to PENDING for retry
            for task in execution.tasks.values():
                if task.state == TaskState.RUNNING:
                    task.state = TaskState.PENDING
                    task.start_time = None
            
            # Resume execution if not complete
            if execution.state == "RUNNING":
                asyncio.create_task(self._execute_workflow(execution_id))
            
            return True
            
        except Exception as e:
            print(f"Failed to recover execution {execution_id}: {str(e)}")
            return False
    
    def get_execution_status(self, execution_id: str) -> Optional[dict]:
        """Get current status of an execution"""
        if execution_id not in self.executions:
            return None
        
        execution = self.executions[execution_id]
        task_summary = defaultdict(int)
        
        for task in execution.tasks.values():
            task_summary[task.state.value] += 1
        
        return {
            "execution_id": execution_id,
            "version": execution.version,
            "state": execution.state,
            "start_time": execution.start_time,
            "end_time": execution.end_time,
            "duration": (execution.end_time or time.time()) - execution.start_time,
            "task_summary": dict(task_summary),
            "total_tasks": len(execution.tasks)
        }
    
    def list_executions(self) -> List[dict]:
        """List all executions"""
        return [
            self.get_execution_status(exec_id)
            for exec_id in self.executions.keys()
        ]


async def demo_dynamic_task_handler(execution: WorkflowExecution, task: Task) -> dict:
    """Custom handler that generates dynamic tasks"""
    await asyncio.sleep(task.runtime)
    
    # Generate dynamic tasks
    dynamic_tasks = [
        {
            "id": f"validate_{i}",
            "dependencies": [task.id],
            "runtime": 1,
            "dynamic": False
        }
        for i in range(1, 3)
    ]
    
    return {
        "status": "completed",
        "task_id": task.id,
        "dynamic_tasks": dynamic_tasks
    }


async def main():
    """Demonstration of workflow engine capabilities"""
    engine = WorkflowEngine()
    
    engine.register_task_handler("clean_data", demo_dynamic_task_handler)
    
    # Define workflow v1
    workflow_v1 = {
        "version": "v1",
        "name": "data_pipeline",
        "tasks": [
            {"id": "fetch_data", "dependencies": [], "runtime": 2, "dynamic": False},
            {"id": "clean_data", "dependencies": ["fetch_data"], "runtime": 1, "dynamic": True},
            {"id": "log_result", "dependencies": ["clean_data"], "runtime": 1, "dynamic": False}
        ]
    }
    
    # Define workflow v2
    workflow_v2 = {
        "version": "v2",
        "name": "data_pipeline",
        "tasks": [
            {"id": "fetch_data", "dependencies": [], "runtime": 1, "dynamic": False},
            {"id": "transform_data", "dependencies": ["fetch_data"], "runtime": 2, "dynamic": False},
            {"id": "clean_data", "dependencies": ["transform_data"], "runtime": 1, "dynamic": True},
            {"id": "log_result", "dependencies": ["clean_data"], "runtime": 1, "dynamic": False}
        ]
    }
    
    print("=" * 80)
    print("Starting concurrent execution of two workflow versions")
    print("=" * 80)
    
    exec_id_v1 = await engine.submit_workflow(workflow_v1)
    exec_id_v2 = await engine.submit_workflow(workflow_v2)
    
    print(f"\nSubmitted executions:")
    print(f"  V1: {exec_id_v1}")
    print(f"  V2: {exec_id_v2}")
    
    while True:
        await asyncio.sleep(1)
        
        status_v1 = engine.get_execution_status(exec_id_v1)
        status_v2 = engine.get_execution_status(exec_id_v2)
        
        print(f"\nStatus Update:")
        print(f"  V1 ({status_v1['version']}): {status_v1['state']} - Tasks: {status_v1['task_summary']}")
        print(f"  V2 ({status_v2['version']}): {status_v2['state']} - Tasks: {status_v2['task_summary']}")
        
        if status_v1['state'] != "RUNNING" and status_v2['state'] != "RUNNING":
            break
    
    print("\n" + "=" * 80)
    print("Final Results:")
    print("=" * 80)
    
    for exec_id in [exec_id_v1, exec_id_v2]:
        status = engine.get_execution_status(exec_id)
        print(f"\nExecution: {exec_id}")
        print(f"  Version: {status['version']}")
        print(f"  State: {status['state']}")
        print(f"  Duration: {status['duration']:.2f}s")
        print(f"  Total Tasks: {status['total_tasks']}")
        print(f"  Task Summary: {status['task_summary']}")
    
    # Demonstrate recovery
    print("\n" + "=" * 80)
    print("Demonstrating crash recovery...")
    print("=" * 80)
    
    # Create new engine instance (simulating restart)
    new_engine = WorkflowEngine()
    new_engine.register_task_handler("clean_data", demo_dynamic_task_handler)
    
    # Recover executions
    recovered_v1 = await new_engine.recover_execution(exec_id_v1)
    recovered_v2 = await new_engine.recover_execution(exec_id_v2)
    
    print(f"\nRecovery status:")
    print(f"  V1: {'✓' if recovered_v1 else '✗'}")
    print(f"  V2: {'✓' if recovered_v2 else '✗'}")


if __name__ == "__main__":
    asyncio.run(main())
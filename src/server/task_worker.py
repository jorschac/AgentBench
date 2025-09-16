import argparse
import asyncio
import traceback
from asyncio.exceptions import TimeoutError, CancelledError
from typing import TypeVar

import aiohttp
import uvicorn
from fastapi import FastAPI, HTTPException, APIRouter

from src.configs import ConfigLoader
from src.typings import *
from .task import Task, Session


class RunningSampleData:
    index: int
    session_id: int
    session: Session
    asyncio_task: asyncio.Task

    def __init__(self, index, session_id, session, task):
        self.index = index
        self.session_id = session_id
        self.session = session
        self.asyncio_task = task


_T = TypeVar("_T")


# 从 .yaml 中读取 task 信息，创建 TaskWorker 实例
# 启动 FastAPI 应用，监听端口，处理请求，每个 wroker 都是一个服务节点，等待后续测试流程中调用
# 把自己注册进 controller 里。

class TaskWorker:
    def __init__(
        self,
        task: Task,
        router: APIRouter,
        controller_address=None,
        self_address=None,
        heart_rate=8,
        register=True,
    ) -> None:
        # 会话映射表：session_id -> 运行中的样本数据
        self.session_map: Dict[int, RunningSampleData] = dict()
        # 会话操作的异步锁，防止并发访问冲突
        self.session_lock = None
        # FastAPI 应用实例
        self.app = app
        # 具体的任务实例（如 OS、DB、WebShop 等）
        self.task = task
        # Controller 服务的地址
        self.controller_address = controller_address
        # 当前 Worker 自己的地址
        self.self_address = self_address
        # 心跳频率（秒）
        self.heart_rate = heart_rate
        # 心跳线程（暂未使用）
        self.heart_thread = None
        # FastAPI 路由器
        self.router = router

        # 注册 GET 路由：获取任务可用的测试用例索引
        self.router.get("/get_indices")(self.get_indices)
        # 注册 GET 路由：获取当前活跃的会话列表
        self.router.get("/get_sessions")(self.get_sessions)
        # 注册 GET 路由：获取 Worker 状态（并发数等）
        self.router.get("/worker_status")(self.worker_status)
        # 注册 POST 路由：获取特定样本的执行状态
        self.router.post("/sample_status")(self.sample_status)
        # 注册 POST 路由：启动新的测试样本
        self.router.post("/start_sample")(self.start_sample)
        # 注册 POST 路由：Agent 与任务环境交互
        self.router.post("/interact")(self.interact)
        # 注册 POST 路由：取消特定会话
        self.router.post("/cancel")(self.cancel)
        # 注册 POST 路由：取消所有会话
        self.router.post("/cancel_all")(self.cancel_all)
        # 注册 POST 路由：计算整体评估结果
        self.router.post("/calculate_overall")(self.calculate_overall)

        # 注册启动事件：初始化异步锁
        self.router.on_event("startup")(self._initialize)
        # 注册关闭事件：释放任务资源
        self.router.on_event("shutdown")(self.shutdown)

        # 如果需要注册到 Controller，则在启动时执行注册
        if register:
            self.router.on_event("startup")(self.register)

    def _initialize(self):
        """初始化异步锁，在服务启动时调用"""
        self.session_lock = asyncio.Lock()

    async def _call_controller(self, api: str, data: dict):
        """向 Controller 发送 HTTP 请求的通用方法
        
        Args:
            api: API 端点路径
            data: 请求体数据
            
        Returns:
            Controller 响应的 JSON 数据
            
        Raises:
            HTTPException: 当 Controller 返回错误时
        """
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.controller_address + api,
                json=data,
            ) as response:
                if response.status != 200:
                    raise HTTPException(
                        400,
                        "Error: Controller returned error"
                        + "\n"
                        + (await response.text()),
                    )
                result = await response.json()
        return result

    async def register(self):
        """向 Controller 注册当前 Worker，启动心跳任务"""
        asyncio.create_task(self.heart_beat())

    async def heart_beat(self):
        """定期向 Controller 发送心跳，报告 Worker 状态和可用性
        
        发送的信息包括：
        - 任务名称
        - Worker 地址
        - 并发能力
        - 可用的测试用例索引
        """
        while True:
            try:
                await self._call_controller(
                    "/receive_heartbeat",
                    {
                        "name": self.task.name,
                        "address": self.self_address,
                        "concurrency": self.task.concurrency,
                        "indices": self.task.get_indices(),
                    },
                )
            except Exception as e:
                print("Heartbeat failed:", e)
            await asyncio.sleep(self.heart_rate)

    async def task_start_sample_wrapper(self, index: SampleIndex, session: Session, session_id: int):
        """任务执行的包装器，处理任务启动和异常情况
        
        Args:
            index: 测试样本索引
            session: 会话对象
            session_id: 会话 ID
            
        功能：
        - 执行具体的任务逻辑
        - 捕获异常并报告错误状态
        - 任务完成后清理会话并报告结果
        """
        try:
            result = await self.task.start_sample(index, session)
        except Exception as _:
            self.session_map.pop(session_id)
            error = traceback.format_exc()
            await session.controller.env_finish(TaskOutput(
                index=index,
                status=SampleStatus.TASK_ERROR,
                result=error,
                history=session.history,
            ))
            return
        self.session_map.pop(session_id)
        await session.controller.env_finish(TaskOutput(
            index=index,
            status=result.status,
            result=result.result,
            history=session.history,
        ))

    async def start_sample(self, parameters: WorkerStartSampleRequest):
        """启动新的测试样本会话
        
        Args:
            parameters: 包含 session_id 和 index 的启动请求
            
        Returns:
            包含 session_id 和初始环境输出的响应
            
        Raises:
            HTTPException: 当会话 ID 重复或超过并发限制时
            
        流程：
        1. 检查会话 ID 唯一性和并发限制
        2. 创建新会话并启动任务执行器
        3. 等待任务环境的初始输出
        """
        print("job received")
        async with self.session_lock:
            if parameters.session_id in self.session_map:
                raise HTTPException(status_code=400, detail="Session ID already exists")
            print("session map:", self.session_map)
            if len(self.session_map) >= self.task.concurrency:
                raise HTTPException(
                    status_code=406,
                    detail="Sample concurrency limit reached: %d" % self.task.concurrency,
                )
            session = Session()
            print("session created")
            task_executor = self.task_start_sample_wrapper(
                parameters.index, session, parameters.session_id
            )
            t = asyncio.get_event_loop().create_task(task_executor)
            self.session_map[parameters.session_id] = RunningSampleData(
                index=parameters.index,
                session_id=parameters.session_id,
                session=session,
                task=t,
            )

        print("about to pull agent")
        env_output = await session.controller.agent_pull()
        print("output got")
        return {
            "session_id": parameters.session_id,
            "output": env_output.dict(),
        }

    async def interact(self, parameters: InteractRequest):
        """处理 Agent 与任务环境的交互
        
        Args:
            parameters: 包含 session_id 和 agent_response 的交互请求
            
        Returns:
            包含 session_id 和环境响应的结果
            
        Raises:
            HTTPException: 当会话不存在、任务正在执行或发生任务错误时
            
        流程：
        1. 验证会话存在性和可用性
        2. 将 Agent 响应传递给任务环境
        3. 返回环境的反馈
        """
        print("interacting")
        async with self.session_lock:
            running = self.session_map.get(parameters.session_id, None)
        if running is None:
            raise HTTPException(status_code=400, detail="No such session")
        if running.session.controller.agent_lock.locked():
            raise HTTPException(
                status_code=400,
                detail="Task Executing, please do not send new request.",
            )
        print("awaiting agent pull in interact")
        response = await running.session.controller.agent_pull(
            parameters.agent_response
        )
        if response.status == SampleStatus.TASK_ERROR:
            raise HTTPException(status_code=501, detail={
                "session_id": parameters.session_id,
                "output": response.dict(),
            })
        return {
            "session_id": parameters.session_id,
            "output": response.dict(),
        }

    async def cancel_all(self):
        """取消所有正在运行的会话
        
        并发地取消所有活跃会话，清理所有资源
        """
        async with self.session_lock:
            sessions = list(self.session_map.keys())
            cancelling = []
            for session_id in sessions:
                cancelling.append(self.cancel(CancelRequest(session_id=session_id)))
        await asyncio.gather(*cancelling)

    async def cancel(self, parameters: CancelRequest):
        """取消指定的会话
        
        Args:
            parameters: 包含要取消的 session_id 的请求
            
        Returns:
            包含 session_id 的确认响应
            
        Raises:
            HTTPException: 当会话不存在时
            
        流程：
        1. 设置取消状态并释放环境信号
        2. 等待任务自然结束（最多30秒）
        3. 超时则强制取消并清理资源
        """
        async with self.session_lock:
            if parameters.session_id not in self.session_map:
                raise HTTPException(status_code=400, detail="No such session")
            running = self.session_map.get(parameters.session_id)
            print("canceling", running)
            running.session.controller.env_input = AgentOutput(status=AgentOutputStatus.CANCELLED)
            running.session.controller.env_signal.release()
            print("awaiting task")
            try:
                await asyncio.wait_for(running.asyncio_task, timeout=30)
            except (TimeoutError, CancelledError):
                print("Warning: Task Hard Cancelled")
                self.session_map.pop(parameters.session_id)
            return {
                "session_id": parameters.session_id,
            }

    async def worker_status(self):
        """获取 Worker 当前状态
        
        Returns:
            包含最大并发数和当前运行会话数的状态信息
        """
        return {
            "concurrency": self.task.concurrency,
            "current": len(self.session_map),
        }

    async def sample_status(self, parameters: SampleStatusRequest):
        """获取指定样本的执行状态
        
        Args:
            parameters: 包含 session_id 的状态查询请求
            
        Returns:
            包含会话 ID、样本索引和执行状态的信息
            
        Raises:
            HTTPException: 当指定的会话不存在时
        """
        async with self.session_lock:
            if parameters.session_id not in self.session_map:
                raise HTTPException(status_code=400, detail="No such session")
            running = self.session_map[parameters.session_id]
        return {
            "session_id": parameters.session_id,
            "index": running.index,
            "status": running.session.controller.get_status(),
        }

    async def get_sessions(self):
        """获取所有活跃会话的映射
        
        Returns:
            字典，键为 session_id，值为对应的样本索引
        """
        return {sid: session.index for sid, session in self.session_map.items()}

    async def get_indices(self):
        """获取当前任务支持的所有测试用例索引
        
        Returns:
            任务可用的测试用例索引列表
        """
        return self.task.get_indices()

    async def calculate_overall(self, request: CalculateOverallRequest):
        """计算整体评估结果
        
        Args:
            request: 包含测试结果列表的计算请求
            
        Returns:
            任务的整体评估指标和统计信息
        """
        return self.task.calculate_overall(request.results)

    async def shutdown(self):
        """关闭 Worker，释放任务相关的所有资源"""
        self.task.release()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("name", type=str, help="Task name")
    parser.add_argument(
        "--config", "-c", type=str, default="configs/tasks/task_assembly.yaml"
    )
    parser.add_argument(
        "--controller", "-C", type=str, default="http://localhost:5000/api"
    )
    parser.add_argument("--self", "-s", type=str, default="http://localhost:5001/api")
    parser.add_argument("--port", "-p", type=int, default=5001)

    args = parser.parse_args()

    conf = ConfigLoader().load_from(args.config)
    asyncio_task = InstanceFactory.parse_obj(conf[args.name]).create()

    app = FastAPI()
    router_ = APIRouter()
    task_worker = TaskWorker(
        asyncio_task,
        router_,
        controller_address=args.controller,
        self_address=args.self,
    )
    app.include_router(router_, prefix="/api")
    uvicorn.run(app=app, host="0.0.0.0", port=args.port)

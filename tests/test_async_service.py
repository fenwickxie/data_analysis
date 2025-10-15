import pytest
import asyncio
from data_analysis.analysis_service import AsyncDataAnalysisService

@pytest.mark.asyncio
async def test_async_service_basic(monkeypatch):
    # 模拟Kafka客户端
    class DummyConsumer:
        def __init__(self):
            self._started = False
            self._stopped = False
            self._msgs = []
        async def start(self): self._started = True
        async def getone(self):
            if self._msgs:
                return self._msgs.pop(0)
            await asyncio.sleep(0.01)
            return None
        async def stop(self): self._stopped = True
    class DummyProducer:
        def __init__(self): self._started = False; self._stopped = False; self.sent = []
        async def start(self): self._started = True
        async def send(self, topic, value): self.sent.append((topic, value))
        async def stop(self): self._stopped = True
    monkeypatch.setattr('data_analysis.async_service.AsyncKafkaConsumerClient', lambda *a, **k: DummyConsumer())
    monkeypatch.setattr('data_analysis.async_service.AsyncKafkaProducerClient', lambda *a, **k: DummyProducer())
    service = AsyncDataAnalysisService(module_name="load_prediction")
    # 注入模拟消息
    service.consumer._msgs.append(type('msg', (), {'topic': 'T', 'value': {'station_id': 'S', 'x': 1}})())
    async def cb(station_id, module_input):
        return {"result": 42}
    await service.start(callback=cb)
    await asyncio.sleep(0.05)
    await service.stop()
    assert service.producer.sent
    assert service.get_station_status()['S']['running'] is False

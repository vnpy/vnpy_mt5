import threading
from datetime import datetime
from typing import Callable
from zoneinfo import ZoneInfo

import zmq
import zmq.auth

from vnpy_evo.event import EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    OrderType,
    Product,
    Status,
    Interval
)
from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest,
    BarData
)


# MT5 constants
PERIOD_M1: int = 1
PERIOD_H1: int = 16385
PERIOD_D1: int = 16408

FUNCTION_QUERYCONTRACT: int = 0
FUNCTION_QUERYORDER: int = 1
FUNCTION_QUERYHISTORY: int = 2
FUNCTION_SUBSCRIBE: int = 3
FUNCTION_SENDORDER: int = 4
FUNCTION_CANCELORDER: int = 5

ORDER_STATE_STARTED: int = 0
ORDER_STATE_PLACED: int = 1
ORDER_STATE_CANCELED: int = 2
ORDER_STATE_PARTIAL: int = 3
ORDER_STATE_FILLED: int = 4
ORDER_STATE_REJECTED: int = 5

POSITION_TYPE_BUY: int = 0
POSITION_TYPE_SELL: int = 1

TRADE_TRANSACTION_ORDER_ADD: int = 0
TRADE_TRANSACTION_ORDER_UPDATE: int = 1
TRADE_TRANSACTION_ORDER_DELETE: int = 2
TRADE_TRANSACTION_HISTORY_ADD: int = 6
TRADE_TRANSACTION_REQUEST: int = 10

TRADE_RETCODE_MARKET_CLOSED: int = 10018

TYPE_BUY: int = 0
TYPE_SELL: int = 1
TYPE_BUY_LIMIT: int = 2
TYPE_SELL_LIMIT: int = 3
TYPE_BUY_STOP: int = 4
TYPE_SELL_STOP: int = 5

# Order status map
STATUS_MT2VT: dict[int, Status] = {
    ORDER_STATE_STARTED: Status.SUBMITTING,
    ORDER_STATE_PLACED: Status.NOTTRADED,
    ORDER_STATE_CANCELED: Status.CANCELLED,
    ORDER_STATE_PARTIAL: Status.PARTTRADED,
    ORDER_STATE_FILLED: Status.ALLTRADED,
    ORDER_STATE_REJECTED: Status.REJECTED
}

# Order type map
ORDERTYPE_MT2VT: dict[int, tuple] = {
    TYPE_BUY: (Direction.LONG, OrderType.MARKET),
    TYPE_SELL: (Direction.SHORT, OrderType.MARKET),
    TYPE_BUY_LIMIT: (Direction.LONG, OrderType.LIMIT),
    TYPE_SELL_LIMIT: (Direction.SHORT, OrderType.LIMIT),
    TYPE_BUY_STOP: (Direction.LONG, OrderType.STOP),
    TYPE_SELL_STOP: (Direction.SHORT, OrderType.STOP),
}
ORDERTYPE_VT2MT: dict[tuple, int] = {v: k for k, v in ORDERTYPE_MT2VT.items()}

# Bar interval map
INTERVAL_VT2MT: dict[Interval, int] = {
    Interval.MINUTE: PERIOD_M1,
    Interval.HOUR: PERIOD_H1,
    Interval.DAILY: PERIOD_D1,
}

# Timezone
CHINA_TZ: ZoneInfo = ZoneInfo("Asia/Shanghai")
UTC_TZ: ZoneInfo = ZoneInfo("UTC")


class Mt5Gateway(BaseGateway):
    """
    The MT5 trading gateway for VeighNa.
    """

    default_setting: dict[str, str] = {
        "Server Host": "localhost",
        "REQ Port": "6888",
        "SUB Port": "8666",
    }

    exchanges: list[Exchange] = [Exchange.OTC]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "MT5") -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.callbacks: dict[str, Callable] = {
            "account": self.on_account_info,
            "price": self.on_price_info,
            "order": self.on_order_info,
            "position": self.on_position_info
        }

        self.client = Mt5Client(self)
        self.order_count = 0

        self.local_sys_map: dict[str, str] = {}
        self.sys_local_map: dict[str, str] = {}
        self.position_symbols: set[str] = set()

        self.orders: dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        address: str = setting["Server Host"]
        req_port: str = setting["REQ Port"]
        sub_port: str = setting["SUB Port"]

        req_address: str = f"tcp://{address}:{req_port}"
        sub_address: str = f"tcp://{address}:{sub_port}"

        self.client.start(req_address, sub_address)

        self.query_contract()
        self.query_order()

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        mt5_req: dict = {
            "type": FUNCTION_SUBSCRIBE,
            "symbol": req.symbol.replace('-', '.')
        }
        self.client.send_request(mt5_req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        cmd: int = ORDERTYPE_VT2MT.get((req.direction, req.type), None)

        if req.type == OrderType.FOK or req.type == OrderType.FAK or req.type == OrderType.RFQ:
            self.write_log(f"Order type not supported: {req.type.value}")
            return ""

        local_id: str = self.new_orderid()

        mt5_req: dict = {
            "type": FUNCTION_SENDORDER,
            "symbol": req.symbol.replace('-', '.'),
            "cmd": cmd,
            "price": req.price,
            "volume": req.volume,
            "comment": local_id,
        }

        packet: dict = self.client.send_request(mt5_req)
        result: bool = packet["data"]["result"]
        comment: str = packet["data"]["comment"]

        order: OrderData = req.create_order_data(local_id, self.gateway_name)
        if result:
            order.status = Status.SUBMITTING
        else:
            order.status = Status.REJECTED
            self.write_log(f"Order {local_id} is rejected, reason: {comment}")

        self.on_order(order)
        self.orders[local_id] = order

        return order.vt_orderid

    def new_orderid(self) -> str:
        """Generate local order id"""
        prefix: str = datetime.now().strftime("%Y%m%d_%H%M%S_")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(4, "0")

        orderid: str = prefix + suffix
        return orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        if req.orderid not in self.local_sys_map:
            self.write_log(f"Cancel order failed, no system order id is found for {req.orderid}")
            return

        sys_id: str = self.local_sys_map[req.orderid]

        mt5_req: dict = {
            "type": FUNCTION_CANCELORDER,
            "ticket": int(sys_id)
        }

        packet: dict = self.client.send_request(mt5_req)
        result: bool = packet["data"]["result"]

        if result is True:
            self.write_log(f"Cancel order successful {req.orderid}")
        elif result is False:
            self.write_log(f"Cancel order failed: {req.orderid}")

    def query_contract(self) -> None:
        """Query available contracts"""
        mt5_req: dict = {"type": FUNCTION_QUERYCONTRACT}
        packet: dict = self.client.send_request(mt5_req)

        if packet:
            self.write_log("MT5 is connected.")

        for d in packet["data"]:
            contract: ContractData = ContractData(
                symbol=d["symbol"].replace('.', '-'),
                exchange=Exchange.OTC,
                name=d["symbol"].replace('.', '-'),
                product=Product.FOREX,
                size=d["lot_size"],
                pricetick=pow(10, -d["digits"]),
                min_volume=d["min_lot"],
                net_position=True,
                stop_supported=True,
                history_data=True,
                gateway_name=self.gateway_name,
            )
            self.on_contract(contract)

        self.write_log("Available contracts data is received")

    def query_order(self) -> None:
        """Query open orders"""
        mt5_req: dict = {"type": FUNCTION_QUERYORDER}
        packet: dict = self.client.send_request(mt5_req)

        for d in packet.get("data", []):
            direction, order_type = ORDERTYPE_MT2VT[d["order_type"]]

            sys_id: str = str(d["order"])

            if d["order_comment"]:
                local_id: str = d["order_comment"]
            else:
                local_id: str = sys_id

            self.local_sys_map[local_id] = sys_id
            self.sys_local_map[sys_id] = local_id

            order: OrderData = OrderData(
                symbol=d["symbol"].replace('.', '-'),
                exchange=Exchange.OTC,
                orderid=local_id,
                direction=direction,
                type=order_type,
                price=d["order_price"],
                volume=d["order_volume_initial"],
                traded=d["order_volume_initial"] - d["order_volume_current"],
                status=STATUS_MT2VT.get(d["order_state"], Status.SUBMITTING),
                datetime=generate_local_datetime(d["order_time_setup"]),
                gateway_name=self.gateway_name
            )
            self.orders[local_id] = order
            self.on_order(order)

        self.write_log("Open orders data is received")

    def query_account(self) -> None:
        """Not required since MT5 pushes update"""
        pass

    def query_position(self) -> None:
        """Not required since MT5 pushes update"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        history: list[BarData] = []

        start_time: str = generate_utc_datetime(req.start)
        end_time: str = generate_utc_datetime(req.end)

        mt5_req: dict = {
            "type": FUNCTION_QUERYHISTORY,
            "symbol": req.symbol.replace('-', '.'),
            "interval": INTERVAL_VT2MT[req.interval],
            "start_time": start_time,
            "end_time": end_time,
        }
        packet: dict = self.client.send_request(mt5_req)

        if packet["result"] == -1:
            self.write_log("Query kline history failed")
        else:
            for d in packet["data"]:
                bar: BarData = BarData(
                    symbol=req.symbol.replace('.', '-'),
                    exchange=Exchange.OTC,
                    datetime=generate_china_datetime(d["time"]),
                    interval=req.interval,
                    volume=d["real_volume"],
                    open_price=d["open"],
                    high_price=d["high"],
                    low_price=d["low"],
                    close_price=d["close"],
                    gateway_name=self.gateway_name
                )
                history.append(bar)

            data: dict = packet["data"]
            begin: datetime = generate_china_datetime(data[0]["time"])
            end: datetime = generate_china_datetime(data[-1]["time"])

            msg: str = f"Query kline history finished, {req.symbol.replace('.','-')} - {req.interval.value}, {begin} - {end}"
            self.write_log(msg)

        return history

    def close(self) -> None:
        """Close server connections"""
        self.client.stop()
        self.client.join()

    def callback(self, packet: dict) -> None:
        """General callback for receiving packet data"""
        type_: str = packet["type"]
        callback_func: callable = self.callbacks.get(type_, None)

        if callback_func:
            callback_func(packet)

    def on_order_info(self, packet: dict) -> None:
        """Callback of order update"""
        data: dict = packet["data"]
        if not data["order"]:
            if data["trans_type"] == TRADE_TRANSACTION_REQUEST:
                local_id: str = data["request_comment"]
                order: OrderData = self.orders.get(local_id, None)
                if local_id and order:

                    order_id: str = str(data["result_order"])
                    if data["result_order"] and self.sys_local_map[order_id] == order_id:
                        order.orderid = local_id
                        order.traded = data["result_volume"]
                        if order.traded == order.volume:
                            order.status = Status.ALLTRADED
                        else:
                            order.status = Status.PARTTRADED
                        self.on_order(order)
                        trade: TradeData = TradeData(
                            symbol=order.symbol,
                            exchange=order.exchange,
                            direction=order.direction,
                            orderid=data["request_comment"],
                            tradeid=data["result_deal"],
                            price=data["result_price"],
                            volume=data["result_volume"],
                            datetime=datetime.now(CHINA_TZ),
                            gateway_name=self.gateway_name
                        )
                        self.on_trade(trade)

                    elif data["result_retcode"] == TRADE_RETCODE_MARKET_CLOSED:
                        order.status = Status.REJECTED
                        self.write_log(f"Order {local_id} is rejected, reason: market_closed")
                        self.on_order(order)
            return

        trans_type: int = data["trans_type"]

        # Map system order id to local order id
        if trans_type == TRADE_TRANSACTION_ORDER_ADD:
            sys_id: str = str(data["order"])

            local_id: str = data["order_comment"]
            if not local_id:
                local_id = sys_id

            self.local_sys_map[local_id] = sys_id
            self.sys_local_map[sys_id] = local_id

            order: OrderData = self.orders.get(local_id, None)
            if local_id and order:
                order.datetime = generate_local_datetime(data["order_time_setup"])

        # Update order data
        elif trans_type in {TRADE_TRANSACTION_ORDER_UPDATE, TRADE_TRANSACTION_ORDER_DELETE}:
            sysid: str = str(data["order"])
            local_id: str = self.sys_local_map[sysid]

            order: OrderData = self.orders.get(local_id, None)
            if not order:
                direction, order_type = ORDERTYPE_MT2VT[data["order_type"]]

                order: OrderData = OrderData(
                    symbol=data["symbol"].replace('.', '-'),
                    exchange=Exchange.OTC,
                    orderid=local_id,
                    type=order_type,
                    direction=direction,
                    price=data["order_price"],
                    volume=data["order_volume_initial"],
                    gateway_name=self.gateway_name
                )
                self.orders[local_id] = order

            if data["order_time_setup"]:
                order.datetime = generate_local_datetime(data["order_time_setup"])

            if data["trans_state"] in STATUS_MT2VT:
                order.status = STATUS_MT2VT[data["trans_state"]]

            self.on_order(order)

        # Update trade data
        elif trans_type == TRADE_TRANSACTION_HISTORY_ADD:
            sysid: str = str(data["order"])
            local_id: str = self.sys_local_map[sysid]

            order: OrderData = self.orders.get(local_id, None)
            if order:
                if data["order_time_setup"]:
                    order.datetime = generate_local_datetime(data["order_time_setup"])

                trade: TradeData = TradeData(
                    symbol=order.symbol.replace('.', '-'),
                    exchange=order.exchange,
                    direction=order.direction,
                    orderid=order.orderid,
                    tradeid=data["deal"],
                    price=data["trans_price"],
                    volume=data["trans_volume"],
                    datetime=datetime.now(CHINA_TZ),
                    gateway_name=self.gateway_name
                )
                order.traded = trade.volume
                self.on_order(order)
                self.on_trade(trade)

    def on_account_info(self, packet: dict) -> None:
        """Callback of account balance update"""
        data: dict = packet["data"]

        account: AccountData = AccountData(
            accountid=data["name"],
            balance=data["balance"],
            frozen=data["margin"],
            gateway_name=self.gateway_name
        )
        self.on_account(account)

    def on_position_info(self, packet: dict) -> None:
        """Callback of holding positions update"""
        positions: dict = {}

        data: dict = packet.get("data", [])
        for d in data:
            position: PositionData = PositionData(
                symbol=d["symbol"].replace('.', '-'),
                exchange=Exchange.OTC,
                direction=Direction.NET,
                gateway_name=self.gateway_name
            )

            if d["type"] == POSITION_TYPE_BUY:
                position.volume = d["volume"]
            else:
                position.volume = -d["volume"]

            position.price = d["price"]
            position.pnl = d["current_profit"]

            positions[position.symbol] = position

        for symbol in self.position_symbols:
            if symbol not in positions:
                position: PositionData = PositionData(
                    symbol=symbol,
                    exchange=Exchange.OTC,
                    direction=Direction.NET,
                    gateway_name=self.gateway_name
                )
                positions[symbol] = position

        for position in positions.values():
            self.position_symbols.add(position.symbol)
            self.on_position(position)

    def on_price_info(self, packet: dict) -> None:
        """Callback of market price update"""
        if "data" not in packet:
            return

        for d in packet["data"]:

            tick: TickData = TickData(
                symbol=d["symbol"].replace('.', '-'),
                exchange=Exchange.OTC,
                name=d["symbol"].replace('.', '-'),
                bid_price_1=d["bid"],
                ask_price_1=d["ask"],
                volume=d["last_volume"],
                datetime=datetime.now(),
                gateway_name=self.gateway_name
            )
            if tick.last_price:
                tick.last_price = d["last"]
                tick.high_price = d["last_high"]
                tick.low_price = d["last_low"]
            else:
                tick.last_price = (d["bid"] + d["ask"]) / 2
                tick.high_price = (d["bid_high"] + d["ask_high"]) / 2
                tick.low_price = (d["bid_low"] + d["ask_low"]) / 2

            self.on_tick(tick)


class Mt5Client:
    """
    The client for connecting to MT5 server based on ZeroMQ.
    """

    def __init__(self, gateway: Mt5Gateway):
        """
        The init method of the client.

        gateway: the parent gateway object for pushing callback data
        """
        self.gateway: Mt5Gateway = gateway

        self.context: zmq.Context = zmq.Context()
        self.socket_req: zmq.Socket = self.context.socket(zmq.REQ)
        self.socket_sub: zmq.Socket = self.context.socket(zmq.SUB)
        self.socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")

        self.active: bool = False
        self.thread: threading.Thread = None
        self.lock: threading.Lock = threading.Lock()

    def start(self, req_address: str, sub_address: str) -> None:
        """Start ZeroMQ connection"""
        if self.active:
            return

        # Connect ZeroMQ sockets
        self.socket_req.connect(req_address)
        self.socket_sub.connect(sub_address)

        # Start the thread to process incoming data
        self.active: bool = True
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def stop(self) -> None:
        """Stop ZeroMQ connection"""
        if not self.active:
            return
        self.active = False

    def join(self) -> None:
        """Join to wait the thread exit loop"""
        if self.thread and self.thread.is_alive():
            self.thread.join()
        self.thread = None

    def run(self) -> None:
        """Function run in the thread"""
        while self.active:
            if not self.socket_sub.poll(1000):
                continue

            data: dict = self.socket_sub.recv_json(flags=zmq.NOBLOCK)
            self.callback(data)

        # Close ZeroMQ sockets
        self.socket_req.close()
        self.socket_sub.close()

    def callback(self, data: dict) -> None:
        """General callback function"""
        self.gateway.callback(data)

    def send_request(self, req: dict) -> dict:
        """Send request to server"""
        if not self.active:
            return {}

        self.socket_req.send_json(req)
        data: dict = self.socket_req.recv_json()
        return data


def generate_local_datetime(timestamp: int) -> datetime:
    """Generate local datetime"""
    dt: datetime = datetime.fromtimestamp(timestamp)
    dt: datetime = CHINA_TZ.localize(dt)
    return dt


def generate_china_datetime(timestamp: int) -> datetime:
    """Generate China datetime"""
    dt: dict = datetime.strptime(str(timestamp), "%Y.%m.%d %H:%M")
    utc_dt: dict = dt.replace(tzinfo=UTC_TZ)
    china_tz: dict = utc_dt.astimezone(CHINA_TZ)
    return china_tz


def generate_utc_datetime(datetime: datetime) -> str:
    """Generate UTC datetime"""
    utc_tz: dict = datetime.astimezone(UTC_TZ)
    utc_tz: dict = utc_tz.replace(tzinfo=None)
    dt: str = utc_tz.isoformat()
    dt: str = dt.replace('T', ' ')
    return dt

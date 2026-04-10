import asyncio
import json
import re
import sys
import os
import time
import hashlib
from collections import deque, defaultdict
from datetime import datetime, timedelta
from mcp import ClientSession
from mcp.client.sse import sse_client

MCP_URL = "http://localhost:8080/sse"
CACHE_FILE = os.path.join(os.path.dirname(__file__), "train_cache_graph.json")
CACHE_EXPIRE_DAYS = 2  # 边缓存有效期（天）

# -------------------- MCP 核心交互 (不变) --------------------
async def call_mcp_tool(session, tool_name, arguments):
    result = await session.call_tool(tool_name, arguments)
    for c in result.content:
        if c.type == "text":
            return c.text
    return None

async def get_current_date(session):
    text = await call_mcp_tool(session, "get-current-date", {})
    return text.strip() if text else None

def clean_station_name(name):
    suffixes = ["火车站", "高铁站", "站", "车站"]
    for suffix in suffixes:
        if name.endswith(suffix):
            return name[:-len(suffix)]
    return name

async def get_station_code(session, location_name):
    city_text = await call_mcp_tool(session, "get-station-code-of-citys", {"citys": location_name})
    if city_text:
        try:
            data = json.loads(city_text)
            if location_name in data:
                return data[location_name]["station_code"]
        except:
            pass
    cleaned = clean_station_name(location_name)
    if cleaned != location_name:
        station_text = await call_mcp_tool(session, "get-station-code-by-names", {"stationNames": cleaned})
        if station_text:
            try:
                data = json.loads(station_text)
                if isinstance(data, dict) and cleaned in data:
                    return data[cleaned]["station_code"]
                if isinstance(data, str):
                    return data.strip()
            except:
                pass
    station_text = await call_mcp_tool(session, "get-station-code-by-names", {"stationNames": location_name})
    if station_text:
        try:
            data = json.loads(station_text)
            if isinstance(data, dict) and location_name in data:
                return data[location_name]["station_code"]
            if isinstance(data, str):
                return data.strip()
        except:
            pass
    return None

async def query_direct(session, from_code, to_code, date, train_types=None):
    arguments = {"fromStation": from_code, "toStation": to_code, "date": date, "purpose_codes": "ADULT"}
    if train_types:
        arguments["trainFilterFlags"] = train_types
    return await call_mcp_tool(session, "get-tickets", arguments)

async def query_interline(session, from_code, to_code, date, train_types=None):
    arguments = {"fromStation": from_code, "toStation": to_code, "date": date, "purpose_codes": "ADULT"}
    if train_types:
        arguments["trainFilterFlags"] = train_types
    return await call_mcp_tool(session, "get-interline-tickets", arguments)

# -------------------- 文本解析 (不变) --------------------
def parse_direct_blocks(raw_text):
    lines = raw_text.splitlines()
    blocks, current_block = [], []
    train_pattern = re.compile(r'^[GDCZTK]\d+')
    for line in lines:
        if train_pattern.match(line.strip()):
            if current_block: blocks.append('\n'.join(current_block))
            current_block = [line]
        else:
            if current_block: current_block.append(line)
    if current_block: blocks.append('\n'.join(current_block))
    return blocks

def parse_transfer_blocks(raw_text):
    lines = raw_text.splitlines()
    blocks, current_block = [], []
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2} ->')
    for line in lines:
        if date_pattern.match(line):
            if current_block: blocks.append('\n'.join(current_block))
            current_block = [line]
        else:
            if current_block: current_block.append(line)
    if current_block: blocks.append('\n'.join(current_block))
    return blocks

def extract_train_codes(block):
    lines = block.split('\n')
    codes = []
    train_pattern = re.compile(r'^([GDCZTK]\d+)\s+')
    for line in lines:
        match = train_pattern.match(line.strip())
        if match: codes.append(match.group(1))
    return codes

def is_same_train_transfer(block):
    codes = extract_train_codes(block)
    return len(codes) >= 2 and len(set(codes)) == 1

def filter_by_train_types(block, allowed_types):
    if not allowed_types: return True
    for code in extract_train_codes(block):
        if code[0] not in allowed_types: return False
    return True

def filter_by_via(blocks, via_city, exclude_same_train=True, allowed_train_types=None):
    matched = []
    for block in blocks:
        if exclude_same_train and is_same_train_transfer(block): continue
        if not filter_by_train_types(block, allowed_train_types): continue
        first_line = block.split('\n')[0]
        match = re.search(r'\|\s*([^|]+)\s*->\s*([^|]+)\s*->\s*([^|]+)\s*\|', first_line)
        if match:
            transfer_st = match.group(2).strip()
            if via_city in transfer_st: matched.append(block)
    return matched

# -------------------- 图缓存管理 (核心新功能) --------------------
class TrainCacheGraph:
    def __init__(self, filepath=CACHE_FILE):
        self.filepath = filepath
        self.graph = defaultdict(lambda: defaultdict(list))  # {from_code: {to_code: [edge_info]}}
        self.station_names = {}  # code -> name
        self.load()

    def load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 重建图结构
                    for from_code, to_dict in data.get('graph', {}).items():
                        for to_code, edges in to_dict.items():
                            self.graph[from_code][to_code] = edges
                    self.station_names = data.get('station_names', {})
            except:
                pass

    def save(self):
        data = {
            'graph': {from_code: dict(to_dict) for from_code, to_dict in self.graph.items()},
            'station_names': self.station_names
        }
        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def add_edge(self, from_code, to_code, train_info, date, train_type):
        """添加或更新一条边（车次信息）"""
        edge = {
            'train_code': train_info.get('train_code', ''),
            'depart_time': train_info.get('depart_time', ''),
            'arrive_time': train_info.get('arrive_time', ''),
            'duration': train_info.get('duration', ''),
            'seats': train_info.get('seats', {}),
            'cached_date': date,
            'cached_timestamp': time.time(),
            'train_type': train_type
        }
        # 检查是否已存在相同车次，存在则更新时间戳
        existing = self.graph[from_code][to_code]
        for i, e in enumerate(existing):
            if e['train_code'] == edge['train_code']:
                existing[i] = edge
                self.save()
                return
        existing.append(edge)
        self.save()

    def get_edges(self, from_code, to_code, date=None, train_types=None, max_age_days=CACHE_EXPIRE_DAYS):
        """获取两点间的有效缓存边（未过期且符合类型）"""
        edges = self.graph.get(from_code, {}).get(to_code, [])
        valid = []
        now = time.time()
        for e in edges:
            # 检查时效
            if now - e['cached_timestamp'] > max_age_days * 86400:
                continue
            # 检查日期匹配（同日或相近？此处简化，只要求缓存日期与查询日期相同，实际可放宽）
            if date and e['cached_date'] != date:
                continue
            # 检查类型
            if train_types and e['train_type'][0] not in train_types:
                continue
            valid.append(e)
        return valid

    def find_paths(self, start_code, end_code, date, train_types, max_hops=4):
        """在缓存图中搜索路径（BFS）"""
        queue = deque()
        queue.append((start_code, []))
        visited = set()
        found = []
        while queue:
            cur, path = queue.popleft()
            if len(path) >= max_hops:
                continue
            for nxt, edges in self.graph[cur].items():
                if nxt in visited:
                    continue
                valid_edges = [e for e in edges if (not date or e['cached_date'] == date) and
                               (not train_types or e['train_type'][0] in train_types)]
                if not valid_edges:
                    continue
                # 选择第一个有效边作为代表（实际可返回多个方案，这里简化）
                edge = valid_edges[0]
                step = {
                    'from_code': cur,
                    'to_code': nxt,
                    'train_code': edge['train_code'],
                    'depart_time': edge['depart_time'],
                    'arrive_time': edge['arrive_time'],
                    'duration': edge['duration'],
                    'seats': edge['seats']
                }
                new_path = path + [step]
                if nxt == end_code:
                    found.append(new_path)
                else:
                    visited.add(nxt)
                    queue.append((nxt, new_path))
        return found

    def invalidate_edge(self, from_code, to_code, train_code):
        """删除指定边（车次失效时）"""
        if from_code in self.graph and to_code in self.graph[from_code]:
            self.graph[from_code][to_code] = [e for e in self.graph[from_code][to_code] if e['train_code'] != train_code]
            self.save()

    def update_station_name(self, code, name):
        self.station_names[code] = name
        self.save()

# 全局缓存实例
cache_graph = TrainCacheGraph()

# -------------------- 增强的票务获取（集成缓存） --------------------
async def get_tickets_between(session, from_code, to_code, date, train_types, use_cache=True):
    # 先查缓存
    if use_cache:
        cached_edges = cache_graph.get_edges(from_code, to_code, date, train_types)
        if cached_edges:
            # 简单验证：检查第一个车次是否仍有余票（可抽样）
            edge = cached_edges[0]
            # 快速API验证（可选，这里为了效率默认信任缓存）
            # 如需验证，可调用 query_direct 并比对
            print(f"  使用缓存边: {from_code}->{to_code} {edge['train_code']}", file=sys.stderr)
            return {
                'type': 'direct',
                'trains': [format_cached_edge(edge)],
                'from_cache': True
            }
    # 缓存未命中，实时查询
    direct_raw = await query_direct(session, from_code, to_code, date, train_types)
    if direct_raw and "Error:" not in direct_raw:
        blocks = parse_direct_blocks(direct_raw)
        blocks = [b for b in blocks if filter_by_train_types(b, train_types)]
        if blocks:
            # 将车次信息存入缓存图
            for block in blocks[:3]:
                extract_and_cache_edges(block, from_code, to_code, date, train_types)
            return {"type": "direct", "trains": blocks[:5]}
    inter_raw = await query_interline(session, from_code, to_code, date, train_types)
    if inter_raw and "Error:" not in inter_raw:
        blocks = parse_transfer_blocks(inter_raw)
        blocks = [b for b in blocks if not is_same_train_transfer(b) and filter_by_train_types(b, train_types)]
        if blocks:
            return {"type": "interline", "schemes": blocks[:3]}
    return None

def format_cached_edge(edge):
    """将缓存的边数据格式化为类似原始文本块"""
    lines = [
        f"{edge['train_code']} {edge['from_code']} -> {edge['to_code']} {edge['depart_time']} -> {edge['arrive_time']} 历时：{edge['duration']}",
    ]
    for seat, info in edge['seats'].items():
        lines.append(f"- {seat}: {info}")
    return '\n'.join(lines)

def extract_and_cache_edges(block, from_code, to_code, date, train_types):
    """从原始文本块中提取车次信息并存入缓存图"""
    lines = block.split('\n')
    first_line = lines[0]
    match = re.match(r'^([GDCZTK]\d+)\s+.*?(\d{2}:\d{2})\s*->\s*(\d{2}:\d{2})\s+历时：(\d{2}:\d{2})', first_line)
    if not match:
        return
    train_code = match.group(1)
    depart_time = match.group(2)
    arrive_time = match.group(3)
    duration = match.group(4)
    seats = {}
    for line in lines[1:]:
        seat_match = re.match(r'-\s*(\S+):\s*(.*)', line)
        if seat_match:
            seats[seat_match.group(1)] = seat_match.group(2)
    edge_info = {
        'train_code': train_code,
        'depart_time': depart_time,
        'arrive_time': arrive_time,
        'duration': duration,
        'seats': seats
    }
    cache_graph.add_edge(from_code, to_code, edge_info, date, train_types[0] if train_types else '')

# -------------------- 智能多跳规划（集成缓存图搜索） --------------------
ZTK_HUBS = {  # 保留，但缓存图优先
    "新乡": "XXF", "郑州": "ZZF", "洛阳": "LYF", "西安": "XAY",
    "武汉": "WHN", "武昌": "WCN", "汉口": "HKN", "长沙": "CSQ",
    "株洲": "ZZQ", "衡阳": "HYQ", "怀化": "HHQ", "贵阳": "GIW",
    "昆明": "KMM", "成都": "CDW", "重庆": "CQW", "六盘水": "UMW",
    "石家庄": "SJP", "北京": "BJP", "北京西": "BXP", "驻马店": "ZDN",
    "襄阳": "XFN", "南昌": "NCG", "合肥": "HFH"
}

async def auto_plan_route(session, start_station, end_station, date, train_types="ZTK", max_hops=4, force_refresh=False):
    start_code = await get_station_code(session, start_station)
    end_code = await get_station_code(session, end_station)
    if not start_code or not end_code:
        return {"error": "无法获取起点/终点代码"}
    cache_graph.update_station_name(start_code, start_station)
    cache_graph.update_station_name(end_code, end_station)

    # 1. 优先在缓存图中搜索路径
    if not force_refresh:
        cached_paths = cache_graph.find_paths(start_code, end_code, date, train_types, max_hops)
        if cached_paths:
            print(f"✅ 从缓存图中找到 {len(cached_paths)} 条路径", file=sys.stderr)
            # 快速验证第一条路径的关键边（可选）
            # 此处为简洁，直接信任缓存
            routes = []
            for path in cached_paths[:3]:
                legs = []
                for step in path:
                    legs.append({
                        "from": cache_graph.station_names.get(step['from_code'], step['from_code']),
                        "to": cache_graph.station_names.get(step['to_code'], step['to_code']),
                        "from_code": step['from_code'],
                        "to_code": step['to_code'],
                        "type": "direct",
                        "details": [format_cached_edge(step)]
                    })
                routes.append(legs)
            return {
                "query_type": "auto_plan",
                "date": date,
                "from": start_station,
                "to": end_station,
                "train_types": train_types,
                "max_hops": max_hops,
                "total_routes": len(routes),
                "routes": routes,
                "cached": True
            }

    # 2. 缓存未命中，执行实时BFS探索（利用枢纽站）
    print("🔄 缓存未命中，开始实时探索...", file=sys.stderr)
    code_to_name = {v: k for k, v in ZTK_HUBS.items()}
    queue = deque()
    queue.append((start_code, []))
    visited = set([start_code])
    found_routes = []

    while queue:
        cur_code, path = queue.popleft()
        if len(path) >= max_hops: continue
        for hub_name, hub_code in ZTK_HUBS.items():
            if hub_code == cur_code: continue
            if hub_code in visited: continue
            ticket_info = await get_tickets_between(session, cur_code, hub_code, date, train_types, use_cache=True)
            if not ticket_info: continue
            step = {
                "from": code_to_name.get(cur_code, cur_code),
                "to": hub_name,
                "from_code": cur_code,
                "to_code": hub_code,
                "type": ticket_info["type"],
                "details": ticket_info.get("trains") or ticket_info.get("schemes")
            }
            if hub_code == end_code:
                found_routes.append(path + [step])
            else:
                visited.add(hub_code)
                queue.append((hub_code, path + [step]))
    found_routes.sort(key=lambda r: len(r))
    return {
        "query_type": "auto_plan",
        "date": date,
        "from": start_station,
        "to": end_station,
        "train_types": train_types,
        "max_hops": max_hops,
        "total_routes": len(found_routes),
        "routes": found_routes[:5],
        "cached": False
    }

# -------------------- 原有功能函数（稍作适配） --------------------
async def async_main(session, from_loc, to_loc, date=None, via_city=None, train_types=None):
    if date is None:
        date = await get_current_date(session)
        if not date: return {"error": "无法获取当前日期"}
    from_code = await get_station_code(session, from_loc)
    to_code = await get_station_code(session, to_loc)
    if not from_code or not to_code:
        return {"error": f"无法获取车站代码"}

    if via_city:
        raw = await query_interline(session, from_code, to_code, date, train_types)
        if not raw or "Error:" in raw: return {"error": raw or "无中转方案"}
        blocks = parse_transfer_blocks(raw)
        filtered = filter_by_via(blocks, via_city, True, train_types)
        return {"query_type": "interline_with_via", "date": date, "from": from_loc, "to": to_loc,
                "via": via_city, "train_types": train_types, "total_schemes": len(blocks),
                "matched_schemes": len(filtered), "schemes": filtered}
    direct_raw = await query_direct(session, from_code, to_code, date, train_types)
    if direct_raw and "Error:" not in direct_raw:
        blocks = [b for b in parse_direct_blocks(direct_raw) if filter_by_train_types(b, train_types)]
        if blocks:
            return {"query_type": "direct", "date": date, "from": from_loc, "to": to_loc,
                    "train_types": train_types, "total_trains": len(blocks), "trains": blocks[:20]}
    inter_raw = await query_interline(session, from_code, to_code, date, train_types)
    if not inter_raw or "Error:" in inter_raw: return {"error": "无任何车票信息"}
    blocks = parse_transfer_blocks(inter_raw)
    blocks = [b for b in blocks if not is_same_train_transfer(b) and filter_by_train_types(b, train_types)]
    return {"query_type": "interline_fallback", "date": date, "from": from_loc, "to": to_loc,
            "train_types": train_types, "total_schemes": len(blocks), "schemes": blocks[:10]}

# -------------------- 命令行入口 --------------------
def main():
    if len(sys.argv) < 3:
        print("Usage: python train_query.py <from> <to> [--date yyyy-MM-dd] [--via <city>] [--train-type <types>] [--auto-plan] [--max-hops N] [--refresh]")
        sys.exit(1)
    from_loc, to_loc = sys.argv[1], sys.argv[2]
    date, via_city, train_types, auto_plan, max_hops, force_refresh = None, None, None, False, 4, False
    args = sys.argv[3:]
    i = 0
    while i < len(args):
        if args[i] == "--date" and i+1 < len(args):
            date = args[i+1]; i+=2
        elif args[i] == "--via" and i+1 < len(args):
            via_city = args[i+1]; i+=2
        elif args[i] == "--train-type" and i+1 < len(args):
            train_types = args[i+1].upper(); i+=2
        elif args[i] == "--auto-plan":
            auto_plan = True; i+=1
        elif args[i] == "--max-hops" and i+1 < len(args):
            max_hops = int(args[i+1]); i+=2
        elif args[i] == "--refresh":
            force_refresh = True; i+=1
        else:
            i+=1

    async def run():
        async with sse_client(url=MCP_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                if auto_plan:
                    return await auto_plan_route(session, from_loc, to_loc, date, train_types, max_hops, force_refresh)
                else:
                    return await async_main(session, from_loc, to_loc, date, via_city, train_types)

    result = asyncio.run(run())
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
import asyncio
import ipaddress
import logging
import re
import socket
import subprocess
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("logger")


def get_normalize_mac(mac: str) -> str:
    """Lower-case, zero-padded, colon-separated MAC (macOS arp drops leading zeros)."""
    parts = re.split(r"[:\-]", mac.strip().lower())
    return ":".join(p.zfill(2) for p in parts)


def get_local_network() -> ipaddress.IPv4Network:
    """Best-effort local /24 network, based on the interface used for outbound traffic."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 80))  # UDP connect sends no packets; just selects the interface
        local_ip = sock.getsockname()[0]
    return ipaddress.ip_network(f"{local_ip}/24", strict=False)


async def probe(ip: str, ports: Tuple[int, ...], timeout: float = 0.5) -> None:
    """Attempt a TCP connection to populate the ARP table."""
    for port in ports:
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout)
            writer.close()
            return
        except Exception:
            continue


def read_arp_table() -> Dict[str, str]:
    """Return {normalised_mac: ip} from the system ARP table."""
    table: Dict[str, str] = {}
    try:
        with open("/proc/net/arp") as f:  # Linux / Docker
            for line in list(f)[1:]:
                cols = line.split()
                if len(cols) >= 4 and cols[3] != "00:00:00:00:00:00":
                    table[get_normalize_mac(cols[3])] = cols[0]
        return table
    except FileNotFoundError:
        pass
    out = subprocess.run(["arp", "-an"], capture_output=True, text=True, timeout=10).stdout  # macOS
    for ip, mac in re.findall(r"\(([\d.]+)\) at ([0-9a-fA-F:]+)", out):
        table[get_normalize_mac(mac)] = ip
    return table


async def get_ips_macs(macs: List[str], ports: Tuple[int, ...], concurrency: int = 64) -> Dict[str, Optional[str]]:
    """Scan the local /24 once and look up each MAC. Returns {mac: ip or None} keyed by the MACs as given."""
    network = get_local_network()
    logger.info(f"Scanning {network} for MACs {', '.join(macs)}")

    sem = asyncio.Semaphore(concurrency)

    async def limited_probe(ip: str) -> None:
        async with sem:
            await probe(ip, ports)

    await asyncio.gather(*(limited_probe(str(ip)) for ip in network.hosts()))
    table = await asyncio.to_thread(read_arp_table)
    ips_macs: Dict[str, Optional[str]] = dict()
    for mac in macs:
        ips_macs[mac] = table.get(get_normalize_mac(mac))
    return ips_macs


async def get_ip_mac(mac: str, ports: Tuple[int, ...], concurrency: int = 64) -> Optional[str]:
    """Scan the local /24 for a device with the given MAC. Returns its IP, or None if not found."""
    return (await get_ips_macs([mac], ports=ports, concurrency=concurrency))[mac]

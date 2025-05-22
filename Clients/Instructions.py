from typing_extensions import Self
from dataclasses import dataclass
from hashlib import md5

def start_id_from_instr(instr) -> int:
    cfg = f"{instr.pub_qos}-{instr.delay}-{instr.messagesize}-{instr.instancecount}"
    digest = md5(cfg.encode()).digest()
    return (int.from_bytes(digest, "big") % 10) + 1

@dataclass
class Instructions:
    instancecount   : int | None = None
    pub_qos         : int | None = None
    delay           : int | None = None
    messagesize     : int | None = None
    go              : str | None = None

    def update(s: Self, instruction: str, val: str):
        if not s.check_if_received_full_instruction():
            if instruction in ['instancecount', 'pub_qos', 'delay', 'messagesize']:
                object.__setattr__(s, instruction, int(val))
            if instruction == 'go':
                s.go = 'go'

    def check_if_received_full_instruction(s: Self):
        condition = all(
            [
                isinstance(s.instancecount, int),
                isinstance(s.pub_qos, int),
                isinstance(s.delay, int),
                isinstance(s.messagesize, int),
                isinstance(s.go, str)
            ]
        )
        return True if condition else False

    def check_if_can_start_publishing(s: Self, id: int):
        start = start_id_from_instr(s)
        return ((id-start) % 10) < s.instancecount
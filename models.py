from dataclasses import dataclass


@dataclass
class Module:
    url: str
    name: str
    code: str
    levels: [str]
    duration: str
    nqf_level: int
    credits: int
    purpose: str

    def matches(self, query: str) -> bool:
        q_str = f"{self.name}{self.code}{self.duration}{self.purpose}"
        return q_str.find(query) != -1

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "name": self.name,
            "code": self.code,
            "levels": self.levels,
            "duration": self.duration,
            "nqf_level": self.nqf_level,
            "credits": self.credits,
            "purpose": self.purpose,
        }


@dataclass
class Qualification:
    url: str
    name: str
    stream: str
    code: str
    nqf_level: int
    total_credits: int
    saqa_id: str
    aps_as: int
    purpose_statement: str
    modules: [Module]

    def to_dict(self) -> dict:
        modules = list(map(Module.to_dict, self.modules))
        return {
            "url": self.url,
            "name": self.name,
            "stream": self.stream,
            "code": self.code,
            "nqf_level": self.nqf_level,
            "total_credits": self.total_credits,
            "saqa_id": self.saqa_id,
            "aps_as": self.aps_as,
            "purpose_statement": self.purpose_statement,
            "modules": modules,
        }

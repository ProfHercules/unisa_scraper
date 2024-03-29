from dataclasses import dataclass, field

from bs4 import BeautifulSoup

from pymongo.collection import ObjectId


@dataclass
class Module:
    url: str
    name: str = ""
    code: str = ""
    levels: [str] = field(default_factory=list)
    duration: str = ""
    nqf_level: int = 0
    credits: int = 0
    purpose: str = ""
    pre_requisite: str = ""
    co_requisite: str = ""
    recommendation: str = ""

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
            "pre_requisite": self.pre_requisite,
            "co_requisite": self.co_requisite,
            "recommendation": self.recommendation,
        }


@dataclass
class ModuleGroup:
    heading: str
    modules: [Module]

    def add_module(self, module: Module):
        self.modules.append(module)

    def to_dict(self) -> dict:
        modules = list(map(Module.to_dict, self.modules))
        return {
            "heading": self.heading,
            "modules": modules,
        }


@dataclass
class ModuleLevel:
    module_groups: [ModuleGroup]

    def add_group(self, group: ModuleGroup):
        self.module_groups.append(group)

    def to_dict(self) -> dict:
        module_groups = list(map(ModuleGroup.to_dict, self.module_groups))
        return {
            "module_groups": module_groups
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
    purpose: str
    rules: str
    module_levels: [ModuleLevel]

    def get_num_modules_and_groups(self) -> (int, int):
        modules: int = 0
        groups: int = 0
        for level in self.module_levels:
            groups += len(level.module_groups)
            for group in level.module_groups:
                modules += len(group.modules)

        return modules, groups

    def to_print(self) -> dict:
        modules, groups = self.get_num_modules_and_groups()
        return {
            "name": self.name,
            "stream": self.stream,
            "code": self.code,
            "nqf_level": self.nqf_level,
            "total_credits": self.total_credits,
            "saqa_id": self.saqa_id,
            "aps_as": self.aps_as,
            "purpose": self.rules != "",
            "rules": self.rules != "",
            "module_levels": len(self.module_levels),
            "module_groups": groups,
            "modules": modules
        }

    def to_dict(self) -> dict:
        modules = self.get_num_modules_and_groups()[0]
        module_levels = list(map(ModuleLevel.to_dict, self.module_levels))
        return {
            "url": self.url,
            "name": self.name,
            "stream": self.stream,
            "code": self.code,
            "nqf_level": self.nqf_level,
            "total_credits": self.total_credits,
            "saqa_id": self.saqa_id,
            "aps_as": self.aps_as,
            "purpose": self.purpose,
            "rules": self.rules,
            "module_levels": module_levels,
            "num_modules": modules,
        }

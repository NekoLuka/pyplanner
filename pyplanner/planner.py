import logging
import time
import datetime
import json

from typing import Set, Type, Callable, List, Optional, Union


class CancelJob:
    pass


class Planner:
    jobs: Set["Job"] = {}

    @classmethod
    def every(cls, interval: int = 1) -> "Job":
        return Job(cls, interval)


class Job:
    def __init__(self, planner: Type[Planner] = None, interval: int = None) -> None:
        if planner is None or interval is None:
            raise ValueError("Arguments are not allowed to be None")
        self._planner: Type[Planner] = planner
        self.interval: int = interval
        self.delta_time: int = 0
        self.last_run: int = None
        self.next_run: int = None
        self.start: Optional[int] = None
        self.stop: Optional[int] = None
        self._job: Callable = None
        self._args: Optional[list] = None
        self._kwargs: Optional[dict] = None
        self.tags: Optional[Set[str]] = set()

    @property
    def second(self) -> "Job":
        self.delta_time += self.interval
        return self

    @property
    def seconds(self) -> "Job":
        return self

    @property
    def minute(self) -> "Job":
        self.delta_time += self.interval * 60
        self.interval = self.interval * 60
        return self

    @property
    def minutes(self) -> "Job":
        return self.minute

    @property
    def hour(self) -> "Job":
        self.delta_time += self.interval * 3600
        self.interval = self.interval * 3600
        return self

    @property
    def hours(self) -> "Job":
        return self.hour

    @property
    def day(self) -> "Job":
        self.delta_time += self.interval * 86400
        self.interval = self.interval * 86400
        return self

    @property
    def days(self) -> "Job":
        return self.day

    @property
    def week(self) -> "Job":
        self.delta_time += self.interval * 604800
        self.interval = self.interval * 604800
        return self

    @property
    def weeks(self) -> "Job":
        return self.week

    def starting_from(self, moment: int = None) -> "Job":
        self.start = moment
        return self

    def stopping_at(self, moment: int = None) -> "Job":
        self.stop = moment
        return self

    def do(self, job: Callable, *args, **kwargs) -> "Job":
        self._job = job
        self._args = args
        self._kwargs = kwargs
        return self

    def tag(self, tags: Union[List[str], str]) -> "Job":
        if tags is str:
            self.tags.add(tags)
            return self
        self.tags.update(tags)
        return self

    def commit(self) -> None:
        self.next_run = self.calculate_next_run()
        self._planner.jobs.add(self)

    def run(self) -> Union[Type[CancelJob], None]:
        if self.start and self.now < self.start:
            return None
        if self.stop and self.now > self.stop:
            return CancelJob
        if not self.now >= self.next_run:
            return None

        ret = self._job(*self._args, **self._kwargs)
        if ret is Type[CancelJob]:
            return CancelJob
        self.next_run = self.calculate_next_run()
        self.last_run = self.now
        return None

    def calculate_next_run(self) -> int:
        return self.now + self.delta_time

    @property
    def now(self) -> int:
        return int(time.time())

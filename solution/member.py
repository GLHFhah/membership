from anysystem import Context, Message, Process
import random
from typing import Dict, Set, Tuple

class GroupMember(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._joined = False
        self.T = 5.0
        self.S = 3
        self.K = 2
        self.SAMPLE_SIZE = 20
        self._time = 0
        self._added_members: Dict[str, Tuple[float, int]] = {}
        self._removed_members: Dict[str, Tuple[float, int]] = {}
        self._waiting_answer: Dict[str, int] = {}
        self._active: Set[str] = set()
        self._generation = 0

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'JOIN':
            seed = msg['seed']
            if seed == self._id:
                self._create_group(ctx)
            else:
                self._join_group(seed, ctx)
        elif msg.type == 'LEAVE':
            self._leave_group(ctx)
        elif msg.type == 'GET_MEMBERS':
            members = list(self._get_active_members())
            ctx.send_local(Message('MEMBERS', {'members': members}))

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'PING':
            self._on_ping(msg, sender, ctx)
        elif msg.type == 'PING_ANSWER':
            self._on_ping_answer(msg, sender, ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name.startswith('ping_'):
            target = timer_name[5:]
            status = self._waiting_answer.get(target, 0)
            if status == 1:
                self._waiting_answer[target] = 2
                self._ping_with_retransmitters(target, ctx)
                ctx.set_timer(timer_name, self.T * self.S)
            elif status == 2:
                self._mark_removed(target, ctx.time(), ctx)
                del self._waiting_answer[target]
        elif timer_name == 'periodic_ping':
            self._periodic_ping(ctx)

    def _create_group(self, ctx: Context):
        self._joined = True
        self._mark_added(self._id, ctx.time(), ctx)
        self._active.add(self._id)
        self._start_periodic_ping(ctx)

    def _join_group(self, seed: str, ctx: Context):
        if self._joined:
            return
        self._joined = True
        self._mark_added(self._id, -1.0, ctx)
        self._active.add(self._id)
        for _ in range(self.K):
            msg = Message('PING', {
                'added': self._serialize_sample(self._added_members),
                'removed': self._serialize_sample(self._removed_members),
                'time': self._time
            })
            ctx.send(msg, seed)
        self._start_periodic_ping(ctx)

    def _leave_group(self, ctx: Context):
        if not self._joined:
            return
        self._joined = False
        self._mark_removed(self._id, ctx.time(), ctx)
        self._active.discard(self._id)
        ctx.cancel_timer('periodic_ping')

    def _mark_added(self, proc_id: str, timestamp: float, ctx: Context):
        self._generation += 1
        self._added_members[proc_id] = (timestamp, self._generation)
        if proc_id in self._removed_members:
            if self._added_members[proc_id][0] > self._removed_members[proc_id][0]:
                del self._removed_members[proc_id]
        self._time = max(self._time, self._generation)
        self._active.add(proc_id)

    def _mark_removed(self, proc_id: str, timestamp: float, ctx: Context):
        self._generation += 1
        self._removed_members[proc_id] = (timestamp, self._generation)
        if proc_id in self._added_members:
            if self._removed_members[proc_id][1] > self._added_members[proc_id][1]:
                del self._added_members[proc_id]
        self._time = max(self._time, self._generation)
        self._active.discard(proc_id)

    def _get_active_members(self) -> Set[str]:
        active = set()
        for proc_id in self._added_members:
            removed_entry = self._removed_members.get(proc_id)
            if removed_entry is None:
                active.add(proc_id)
            else:
                added_gen = self._added_members[proc_id][1]
                removed_gen = removed_entry[1]
                if added_gen > removed_gen:
                    active.add(proc_id)
        return active

    def _periodic_ping(self, ctx: Context):
        if not self._joined or not self._active:
            ctx.set_timer('periodic_ping', self.T)
            return
        candidates = [p for p in self._active if p != self._id]
        if not candidates:
            ctx.set_timer('periodic_ping', self.T)
            return
        target = random.choice(candidates)
        self._waiting_answer[target] = 1
        msg = Message('PING', {
            'added': self._serialize_sample(self._added_members),
            'removed': self._serialize_sample(self._removed_members),
            'time': self._time
        })
        ctx.send(msg, target)
        ctx.set_timer(f'ping_{target}', self.T * self.S)
        ctx.set_timer('periodic_ping', self.T)

    def _ping_with_retransmitters(self, target: str, ctx: Context):
        candidates = [p for p in self._active if p != self._id and p != target]
        retransmitters = candidates[:min(self.K, len(candidates))]
        msg = Message('PING', {
            'added': self._serialize_sample(self._added_members),
            'removed': self._serialize_sample(self._removed_members),
            'time': self._time,
            'target': target
        })
        for retransmitter in retransmitters:
            ctx.send(msg, retransmitter)

    def _on_ping(self, msg: Message, sender: str, ctx: Context):
        try:
            added_data = msg['added']
        except:
            added_data = {}
        try:
            removed_data = msg['removed']
        except:
            removed_data = {}
        try:
            time_value = msg['time']
        except:
            time_value = 0
        self._merge_crdt_sample(added_data, removed_data, time_value)
        try:
            target = msg['target']
        except:
            target = None
        if target is not None and target != self._id:
            ctx.send(msg, target)
            return
        if sender not in self._added_members:
            self._mark_added(sender, ctx.time(), ctx)
        elif sender in self._removed_members:
            added_gen = self._added_members.get(sender, (0, -1))[1]
            removed_gen = self._removed_members[sender][1]
            if removed_gen >= added_gen:
                self._mark_added(sender, ctx.time(), ctx)
            self._active.add(sender)
        response = Message('PING_ANSWER', {
            'added': self._serialize_sample(self._added_members),
            'removed': self._serialize_sample(self._removed_members),
            'time': self._time
        })
        ctx.send(response, sender)

    def _on_ping_answer(self, msg: Message, sender: str, ctx: Context):
        try:
            added_data = msg['added']
        except:
            added_data = {}
        try:
            removed_data = msg['removed']
        except:
            removed_data = {}
        try:
            time_value = msg['time']
        except:
            time_value = 0
        self._merge_crdt_sample(added_data, removed_data, time_value)
        if self._waiting_answer.get(sender, 0) > 0:
            self._waiting_answer[sender] = 0
            ctx.cancel_timer(f'ping_{sender}')
        if sender not in self._added_members:
            self._mark_added(sender, ctx.time(), ctx)
        self._active.add(sender)

    def _serialize_sample(self, data_dict: Dict) -> dict:
        items = list(data_dict.items())
        sample_size = min(self.SAMPLE_SIZE, len(items))
        sampled = random.sample(items, sample_size) if sample_size > 0 else []
        return {
            proc_id: {'timestamp': ts, 'generation': gen}
            for proc_id, (ts, gen) in sampled
        }

    def _merge_crdt_sample(self, added_sample: dict, removed_sample: dict, time_value: int):
        self._time = max(self._time, time_value)
        for proc_id, data in added_sample.items():
            ts = data.get('timestamp', 0)
            gen = data.get('generation', 0)
            current_gen = self._added_members.get(proc_id, (0, -1))[1]
            if gen > current_gen:
                self._added_members[proc_id] = (ts, gen)
                if proc_id in self._removed_members:
                    if self._removed_members[proc_id][1] < gen:
                        del self._removed_members[proc_id]
        for proc_id, data in removed_sample.items():
            ts = data.get('timestamp', 0)
            gen = data.get('generation', 0)
            current_gen = self._removed_members.get(proc_id, (0, -1))[1]
            if gen > current_gen:
                self._removed_members[proc_id] = (ts, gen)
                if proc_id in self._added_members:
                    if self._added_members[proc_id][1] < gen:
                        del self._added_members[proc_id]
        self._active = self._get_active_members()

    def _start_periodic_ping(self, ctx: Context):
        ctx.set_timer('periodic_ping', self.T)
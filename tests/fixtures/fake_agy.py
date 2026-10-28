#!/usr/bin/env python3
"""Offline Antigravity protocol fixture; never invokes a model or the network."""
import json
import signal
import sys
import time
from pathlib import Path


def emit(kind, payload):
    print(json.dumps({'event': kind, **payload}), flush=True)


if sys.argv[1:] == ['models']:
    print('Fetching available models...')
    print('fixed-model\tFixture model')
    print('claude-opus-5-5-high\tClaude Opus 5.5 (High)')
    sys.exit(0)

assert '--sandbox' in sys.argv
assert '--dangerously-skip-permissions' not in sys.argv
model = sys.argv[sys.argv.index('--model') + 1]
native = sys.argv[sys.argv.index('--conversation') + 1] if '--conversation' in sys.argv else 'native-agy'
path = Path('.fixture-agy-history.json')
history = json.loads(path.read_text()) if path.exists() else {'turns': 0, 'usage': {}}
counts = {key: 0 for key in ('input_tokens', 'output_tokens', 'cache_read_tokens', 'thinking_tokens', 'total_tokens')}
counts.update(history['usage'])
step_index = history['turns'] * 10
emit('init', {'conversation_id': native, 'init': {'model': 'wrong' if model == 'wrong-init' else model}})


def result(status='SUCCESS', response='done', **extra):
    emit('result', {'result': dict(conversation_id=native, status=status, response=response,
                                   num_turns=history['turns'], usage=counts.copy(), **extra)})


def interrupted(*_):
    result('INTERRUPTED', '')
    sys.exit(0)


signal.signal(signal.SIGINT, interrupted)
for line in sys.stdin:
    event = json.loads(line)
    assert event['event'] == 'user'
    instruction = event['message']['content']
    if instruction == 'quota-zero':
        result('ERROR', '', error={'status': 'RESOURCE_EXHAUSTED', 'retry_after_seconds': 12})
        continue
    previous_turn = history['turns']
    history['turns'] += 1
    step_index += 10
    step = dict(conversation_id=native, step_index=step_index, step_type='user_input', state='DONE')
    emit('step_update', {'step_update': step})
    if instruction == 'hold':
        while True:
            time.sleep(.01)
    if instruction == 'no-result':
        emit('step_update', {'step_update': {**step, 'step_index': step_index + 1, 'step_type': 'agent_response', 'text_delta': 'partial'}})
        sys.exit(0)
    if instruction == 'bad-json':
        print('{broken', flush=True)
        sys.exit(1)
    if instruction == 'wrong-session':
        native = 'different'
    if instruction in {'permission', 'tool'}:
        for phase in ('ACTIVE', 'ACTIVE', 'DONE', 'DONE'):
            info = {'name': 'run_command', 'parameters': {'CommandLine': 'fixture-only'}}
            if instruction == 'permission':
                info['error'] = {'type': 'PERMISSION_DENIED', 'message': 'approval required'}
            emit('step_update', {'step_update': {**step, 'step_index': step_index+1,
                 'step_type': 'tool', 'state': phase, 'tool_name': 'run_command', 'tool_info': info}})
    delta = dict(input_tokens=100, output_tokens=10, thinking_tokens=2, cache_read_tokens=5, total_tokens=110)
    if instruction == 'reset':
        counts = dict.fromkeys(counts, 0)
    for key, value in delta.items():
        counts[key] += value
    emit('step_update', {'step_update': {**step, 'step_index': step_index+2,
         'step_type': 'agent_response', 'text_delta': 'answer', 'usage': delta}})
    response = 'done'
    if instruction.startswith('remember '):
        history['marker'] = instruction.removeprefix('remember ')
        response = 'remembered'
    elif instruction == 'recall':
        response = history.get('marker', 'missing')
    if instruction == 'stale' and previous_turn:
        emit('result', {'result': dict(conversation_id=native, status='SUCCESS', response='OLD',
                                      num_turns=previous_turn, usage=counts)})
    if instruction == 'stderr-denial':
        print('run_command was soft-denied: requires approval', file=sys.stderr, flush=True)
    if instruction == 'quota':
        result('ERROR', '', error={'status': 'RESOURCE_EXHAUSTED', 'retry_after_seconds': 12})
    elif instruction == 'waiting':
        result('WAITING', '')
    elif instruction == 'missing-usage':
        emit('result', {'result': dict(conversation_id=native, status='SUCCESS', response='done', num_turns=history['turns'])})
    else:
        result(response=response)
    if instruction == 'late-stderr-denial':
        print('run_command was soft-denied: requires approval', file=sys.stderr, flush=True)
        for _ in range(30):
            print('unrelated diagnostic', file=sys.stderr, flush=True)
    if instruction == 'bad-exit':
        sys.exit(3)
    history['usage'] = counts.copy()
    path.write_text(json.dumps(history))

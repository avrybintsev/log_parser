#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import heapq
from collections import namedtuple, defaultdict, Counter
#from urlparse import urlparse


def get_lines(filename):
    with open(filename, 'rU') as f:
        for line in f:
            yield line


def get_matches(reader, pattern, processor):
    regex = re.compile(pattern)
    
    for line in reader:
        match = regex.match(line)
        if match is not None:
            yield processor(match)


PATTERN = (
    '(?P<TIME>\d{16})\t' # only for dates after 09.09.2001
    '(?P<ID>\d{1,8})\t' 
    '(?P<TYPE>(StartRequest|BackendConnect|BackendRequest|BackendOk|BackendError|'
    'StartMerge|StartSendResult|FinishRequest))'
    '(?:\t(?P<ADDITIONAL>.*))?'
)

ADDITIONAL_PATTERNS = {
    'StartRequest': None,
    'BackendConnect': '(?P<GR>\d{1,8})\t(?P<URL>.*)',
    'BackendRequest': '(?P<GR>\d{1,8})',
    'BackendOk': '(?P<GR>\d{1,8})',
    'BackendError': '(?P<GR>\d{1,8})\t(?P<ERROR>.*)',
    'StartMerge': None,
    'StartSendResult': None,
    'FinishRequest': None,
}

Match = namedtuple('Match', ('time', 'id', 'type', 'additional'))


def get_processor(additional_patterns={}):
    additional_regex = {k: re.compile(v) for k, v in additional_patterns.iteritems() if v}
    additional_match = lambda t, x: additional_regex[t].match(x).groupdict() if t in additional_regex else x

    def processor(match):
        results = match.groupdict()
        return Match(
            time=long(results['TIME']),
            id=results['ID'],
            type=results['TYPE'],
            additional=additional_match(results['TYPE'], results['ADDITIONAL']),
        )

    return processor


def process_requests(sequence):
    times = []
    send_times = []    
    fails = 0
    backend_ok = defaultdict(Counter)
    backend_error = defaultdict(lambda: defaultdict(Counter))

    url_re = re.compile(r'http:\/\/(?P<NETLOC>[^\/]*).*')
    requests = defaultdict(dict) # buffer for storing request data
    for match in sequence:
        if match.type == 'StartRequest':
            requests[match.id]['start'] = match.time
            requests[match.id]['backends'] = defaultdict(dict)  

        if match.type == 'StartSendResult':
            requests[match.id]['send'] = match.time

        elif match.type.startswith('Backend'):
            gr = match.additional['GR']

            if match.type == 'BackendConnect':
                url_match = url_re.match(match.additional['URL'])
                requests[match.id]['backends'][gr] = url_match.group('NETLOC') if url_match \
                    else match.additional['URL']
                #requests[match.id]['backends'][gr] = urlparse(match.additional['URL']).netloc # too slow!

            elif match.type == 'BackendError':
                url = requests[match.id]['backends'][gr]
                error = match.additional['ERROR']
                backend_error[gr][url][error] += 1

            elif match.type == 'BackendOk':
                url = requests[match.id]['backends'].pop(gr) # if request is ok, remove its url
                backend_ok[gr][url] += 1

        elif match.type == 'FinishRequest':           
            times.append(match.time - requests[match.id]['start'])
            send_times.append((match.id, match.time - requests[match.id]['send']))
            if requests[match.id]['backends']: # if some urls are left => error occured
                fails += 1
            del requests[match.id]
 
    return {
        'p95': times[int(len(times)*0.95)],
        'top10': map(lambda x: str(x[0]), heapq.nlargest(10, send_times, key=lambda x: x[1])),
        'fails': fails,
        'ok': backend_ok,
        'err': backend_error,
    }


def output(filename, data):
    with open(filename, 'w') as f:
        f.write((
            '95-й перцентиль времени работы: {}\n\n' 
            'Идентификаторы запросов с самой долгой фазой отправки результатов пользователю:\n{}\n\n'
            'Запросов с неполным набором ответивших ГР: {}\n\n')
            .format(data['p95'], ' '.join(data['top10']), data['fails'])
        )

        f.write('Обращения и ошибки по бекендам:\n')
        for gr_key in set(data['ok'].viewkeys()) | set(data['err'].viewkeys()):
            f.write('ГР {}:\n'.format(gr_key))
            
            ok = data['ok'][gr_key]
            err = data['err'][gr_key]
            
            for url_key in set(ok.viewkeys()) | set(err.viewkeys()):
                f.write('\t{}\n'.format(url_key))

                total = ok[url_key] + sum(err[url_key].values())
                f.write('\t\tОбращения: {}\n'.format(total))
                
                errors = err[url_key]
                if errors:
                    f.write('\t\tОшибки:\n')
                    for k, v in errors.iteritems():
                        f.write('\t\t\t{}: {}\n'.format(k, v))


def log_analyser(in_file, out_file):
    output(
        out_file, 
        process_requests(
            get_matches(
                reader=get_lines(in_file), 
                pattern=PATTERN, 
                processor=get_processor(additional_patterns=ADDITIONAL_PATTERNS)
            )
        )
    )


log_analyser('input.txt', 'output.txt')

# coding=utf-8
from urlparse import urlparse, parse_qsl
from time import time
from hashlib import md5, sha256
def sortParams(query, now, profile):
    params = []
    for pair in parse_qsl(query, True):
        params.append('{0}={1}'.format(pair[0], pair[1].replace(' ', 'a')))
    params.append('rstr={0}'.format(profile['rstr']))
    params.append('ts={0}'.format(now))
    params.sort()
    result = ''
    for param in params:
        result += param.split('=')[1]
    return result
def shuffle(timex, secret):
    result = ''
    for char in secret:
        result += timex[int(char)]
    return result
def ascp(params, now, profile):
    timex = '{:x}'.format(now)[0:8]
    s1 = shuffle(timex, profile['shuffle'][0])
    s2 = shuffle(timex, profile['shuffle'][1])
    s0 = md5(params).hexdigest()
    if (now % 2 == 1):
        s0 = md5(s0).hexdigest()
    _as = ['_'] * 18
    _as[0] = profile['prefix'][0]
    _as[1] = profile['prefix'][1]
    for i in range(0, 8):
        _as[2 * (i + 1)] = s0[i]
    for i in range(0, 8):
        _as[2 * (i + 1) + 1] = s2[i]
    _cp = ['_'] * 18
    for i in range(0, 8):
        _cp[i * 2] = s1[i]
    for i in range(0, 8):
        _cp[i * 2 + 1] = s0[i + 24]
    _cp[16] = profile['suffix'][0]
    _cp[17] = profile['suffix'][1]
    return {
        'as': ''.join(_as),
        'cp': ''.join(_cp)
    }
def sign_ss(profile, url, form = '', ts = None):  # @UnusedVariable
    parsed = urlparse(url)
    now = ts
    if (now is None):
        now = int(time())
    signature = ascp(sortParams(parsed.query, now, profile), now, profile)
    return {
        'url': url + '&ts={0}&as={1}&cp={2}'.format(now, signature['as'], signature['cp']),
        'form': ''
    }
def sign_gifshow(profile, url, form, ext = None):
    parsed = urlparse(url)
    params = []
    for pair in parse_qsl('{0}&{1}'.format(parsed.query, form), True):
        params.append('{0}={1}'.format(pair[0], pair[1]))
    params.sort()
    to_sign = ''.join(params)
    sig = md5(to_sign + profile['suffix']).hexdigest()
    signed_form = form + '&sig={0}'.format(sig)
    if (ext != None):
        __NStokensig = sha256(sig + ext['token_client_salt']).hexdigest()
        signed_form = signed_form + '&__NStokensig={0}'.format(__NStokensig)
    return {
        'url': url,
        'form': signed_form
    }
aweme = {
    'algorithm': sign_ss,
    'rstr': 'efc84c17',
    'shuffle': ['46107325', '04276153'],
    'prefix': 'a1',
    'suffix': 'e1'
}
hotsoon = {
    'algorithm': sign_ss,
    'rstr': '3ea57347',
    'shuffle': ['46107325', '04276153'],
    'prefix': 'a2',
    'suffix': 'e2'
}
gifshow = {
    'algorithm': sign_gifshow,
    'suffix': '382700b563f4'
}
def sign(url, profile, form = '', ext = None):
    """给url签名
    Args:
        url: 必选；需要签名的url
        profile: 必选；签名算法的配置
        form: 可选；需要签名的body（目前未使用）
    Returns:
        含有签名后url和form的字典
    """
    return profile['algorithm'](profile, url, form, ext)
def better_sign(profile, url, form = '', ext = None):
    return sign(url, profile, form, ext)
# -*- coding=utf-8 -*-
################################################################################
#
# the Huskar framework
# @author dongliqiang@baidu.com
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
#
################################################################################

# error ok
ERRNO_OK = 0
ERRMSG_OK = "ok"
# the framework errno
ERRNO_FRAMEWORK = 9999
ERRMSG_FRAMEWORK = "the framework error"

ERRNO_UNKNOWN = 9998
ERRMSG_UNKNOWN = "unknown error"

ERRNO_NO_MP4 = 9997
ERRMSG_NO_MP4 = "could not get video"

ERRNO_NO_KEYFRAME = 9996
ERRMSG_NO_KEYFRAME = "no key frame"

ERRNO_DOWNLOAD_MP4 = 9995
ERRMSG_DOWNLOAD_MP4 = "could not download mp4"

ERRNO_EXCEED_MAX_MP4_LEN = 9994
ERRMSG_EXCEED_MAX_MP4_LEN = "exceed max content length"

ERRNO_NO_META = 9993
ERRMSG_NO_META = "no meta found"

ERRNO_PARAM = 9992
ERRMSG_PARAM_ERROR = "param error: "

ERRNO_NO_DURATION = 9991
ERRMSG_NO_DURATION = "no duration"

ERRNO_NO_EXCEED_MAX_CONCURRENCY = 9990
ERRMSG_NO_EXCEED_MAX_CONCURRENCY = "exceed max concurrency"

ERRNO_AUDIO_FP_ERROR = 9989
ERRMSG_AUDIO_FP_ERROR = "get audio fingerprint error"

ERRNO_INTERNAL_ERROR = 9988
ERRMSG_INTERNAL_ERROR = "server internal error"


ERRNO_THIRD_ERROR = 9987
ERRMSG_THIRD_ERROR = "3rd internal error"

ERRNO_EXTRACT_PCM = 9986
ERRMSG_EXTRACT_PCM = "error extract pcm"

ERROR = {
    ERRNO_OK: ERRMSG_OK,
    ERRNO_FRAMEWORK: ERRMSG_FRAMEWORK,
    ERRNO_UNKNOWN: ERRMSG_UNKNOWN,
    ERRNO_NO_MP4: ERRMSG_NO_MP4,
    ERRNO_NO_KEYFRAME: ERRMSG_NO_KEYFRAME,
    ERRNO_DOWNLOAD_MP4: ERRMSG_DOWNLOAD_MP4,
    ERRNO_EXCEED_MAX_MP4_LEN: ERRMSG_EXCEED_MAX_MP4_LEN,
    ERRNO_NO_META: ERRMSG_NO_META,
    ERRNO_PARAM: ERRMSG_PARAM_ERROR,
    ERRNO_NO_DURATION:ERRMSG_NO_DURATION,
    ERRNO_NO_EXCEED_MAX_CONCURRENCY: ERRMSG_NO_EXCEED_MAX_CONCURRENCY,
    ERRNO_AUDIO_FP_ERROR: ERRMSG_AUDIO_FP_ERROR,
    ERRNO_INTERNAL_ERROR: ERRMSG_INTERNAL_ERROR,
    ERRNO_THIRD_ERROR: ERRMSG_THIRD_ERROR,
    ERRNO_EXTRACT_PCM:ERRMSG_EXTRACT_PCM
}


class BaseError(Exception):
    """
    every Error should extend BaseError
    """
    def __init__(self, errno=ERRNO_FRAMEWORK, errmsg=None):
        """
        check error or errmsg
        """
        if errno in ERROR:
            self.errno = errno
            if errmsg is None:
                self.errmsg = ERROR[errno]
            else:
                self.errmsg = errmsg
        else:
            self.errno = ERRNO_UNKNOWN
            if errmsg is None:
                self.errmsg = ERROR[ERRNO_UNKNOWN]
            else:
                self.errmsg = errmsg
        
        super(BaseError, self).__init__(errmsg)

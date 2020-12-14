#!/usr/bin/env python3
#
#       timeutils helper methods for working with time() objects for logging high precision latency

#       License:        GNU General Public License (GPL)
#       (c) 2020        Microsoft Corp.
#

# Python modules
from time import time

class TimeUtils:

    @staticmethod
    def getElapsedMilliseconds(startTime: float, endTime: float = None) -> int:
        """
        return number of milliseconds (as int) elapsed since the specified start time
        (where start time is value returned from previous call to time.time())

        if endTime is not specified, then will calculate elapsed against current Time()
        """

        if not endTime:
            endTime = time()

        return int((endTime - startTime) * 1000)
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
from alluxiofs.exception import AlluxioException


class SegmentRange:
    def __init__(self, file_id, segment_index, segment_offset, segment_length):
        self.file_id = file_id
        self.segment_index = segment_index
        self.segment_offset = segment_offset
        self.segment_length = segment_length

    def get_path(self):
        return self.file_id + self.segment_index


class SegmentRanges:
    """ Represent a segment of one file """

    def __init__(self, file_path, segment_size, span_position, span_length):
        self.file_path = file_path
        self.segment_size = segment_size
        self.span_position = span_position
        self.span_length = span_length
        self.segment_num = 0

        self.first_segment_index = span_position / segment_size

        if span_length > 0:
            self.segment_num = 1 + (span_length - self._first_segment_length()) / segment_size
            if ((span_length - self._first_segment_length()) % segment_size) > 0:
                self.segment_num += 1

    def get(self, index):
        if index < 0 or index >= self.segment_num:
            raise AlluxioException(f"index {index} is out of range in segmentRanges of {self.segment_num}")
        if index == 0:
            return SegmentRange(self.file_path, self.first_segment_index, self._first_segment_offset(),
                                self._first_segment_length())
        elif index == self.segment_num - 1:
            return SegmentRange(self.file_path, self.first_segment_index + index, 0, self._last_segment_length())
        else:
            return SegmentRange(self.file_path, self.first_segment_index + index, 0, self.segment_size)

    def size(self):
        return self.segment_num

    def _first_segment_offset(self):
        return self.span_position % self.segment_size

    def _first_segment_length(self):
        return min(self.span_length, self.segment_size - self._first_segment_offset())

    def _last_segment_length(self):
        return self.span_length - self._first_segment_length() - self.segment_size * (self.segment_num - 2)

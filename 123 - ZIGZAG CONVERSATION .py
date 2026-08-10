# Day 123 
# Zigzag Conversation

class Solution:
    def convert(self, s: str, numRows: int) -> str:
        if numRows == 1 or numRows >= len(s):
            return s

        rows = [''] * numRows
        curr_row = 0
        direction = 1  # 1 = down, -1 = up

        for char in s:
            rows[curr_row] += char

            if curr_row == 0:
                direction = 1
            elif curr_row == numRows - 1:
                direction = -1

            curr_row += direction

        return ''.join(rows)
# DAY 174
# Roman To Integer 
# LeetCode 13
# Roman numerals are usually written largest to smallest from left to right. However, the numeral for four is not IIII. Instead, the number four is written as IV. Because the one is before the five we subtract it making four. The same principle applies to the number nine, which is written as IX. There are six instances where subtraction is used:

# Roman numerals are represented by seven different symbols: I, V, X, L, C, D and M.

class Solution:
    def romanToInt(self, s: str) -> int:
        values = {
            'I':1,
            'V':5,
            'X':10,
            'L':50,
            'C':100,
            'D':500,
            'M':1000,        
        }
        total = 0
        for i in range(len(s)):
            if i < len(s) - 1 and values[s[i]] < values[s[i + 1 ]]:
                total -= values[s[i]]
            else : 
                total += values[s[i]]
        
        return total
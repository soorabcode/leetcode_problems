# Day 152 
# Word Search 
# Given an m x n grid of characters board and a string word, return true if word exists in the grid.
# Backtracking on grid; mark visited, explore 4 directions, unmark; graph traversal + backtracking.
class Solution:
    def exist(self, board: List[List[str]], word: str) -> bool:
        m, n = len(board), len(board[0])

        def dfs(r, c, i):
            if i == len(word): return True
            if r < 0 or r >= m or c < 0 or c >= n or board[r][c] != word[i]:
                return False

            ch, board[r][c] = board[r][c], '#'
            ok = any(dfs(r + dr, c + dc, i + 1)
                     for dr, dc in [(1,0),(-1,0),(0,1),(0,-1)])
            board[r][c] = ch
            return ok

        return any(dfs(r, c, 0) for r in range(m) for c in range(n))
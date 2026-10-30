# Day 168 
# Edit Distance ( 72 )
# Given two strings word1 and word2, return the minimum number of operations required to convert word1 to word2.
class Solution:
    def minDistance(self, word1: str, word2: str) -> int:

        n1 = len(word1)
        n2 = len(word2)
        D = [[0] * (n2 + 1) for _ in range(n1 + 1)]
        for i in range(n1 + 1):
            D[i][0] = i
        for j in range(n2 + 1):
            D[0][j] = j

        for i in range(1, n1 + 1):
            for j in range(1, n2 + 1):

                if word1[i - 1] == word2[j - 1]:
                    D[i][j] = D[i - 1][j - 1]

                else:
                    D[i][j] = 1 + min(
                        D[i - 1][j - 1],  # Replace
                        D[i - 1][j],      # Delete
                        D[i][j - 1]       # Insert
                    )
        return D[n1][n2]
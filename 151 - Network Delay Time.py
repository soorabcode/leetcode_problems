# Day 151 
# Network Delay Time 
# You are given a network of n nodes, labeled from 1 to n. You are also given times, a list of travel times as directed edges times[i] = (ui, vi, wi), where ui is the source node, vi is the target node, and wi is the time it takes for a signal to travel from source to target.
# We will send a signal from a given node k. Return the minimum time it takes for all the n nodes to receive the signal. If it is impossible for all the n nodes to receive the signal, return -1.
# Dijkstra’s single-source shortest path; O((V + E) log V ) with min-heap.

class Solution:
    def networkDelayTime(self, times: List[List[int]], n: int, k: int) -> int:
        graph = [[] for _ in range(n + 1)]
        for u, v, w in times:
            graph[u].append((v, w))

        heap = [(0, k)]
        dist = {}

        while heap:
            d, u = heappop(heap)

            if u in dist:
                continue
            dist[u] = d

            for v, w in graph[u]:
                if v not in dist:
                    heappush(heap, (d + w, v))

        return max(dist.values()) if len(dist) == n else -1
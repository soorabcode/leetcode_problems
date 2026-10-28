# Day 166 
# Reorder List
# LeetCode 143 
# You are given the head of a singly linked-list. The list can be represented as:
# L0 → L1 → … → Ln - 1 → Ln
# Reorder the list to be on the following form : L0 → Ln → L1 → Ln - 1 → L2 → Ln - 2 →

# Definition for singly-linked list.
# class ListNode:
#     def __init__(self, val=0, next=None):
#         self.val = val
#         self.next = next
class Solution:
    def reorderList(self, head: ListNode | None) -> None:
        slow = fast = head

        while fast and fast.next:
            slow, fast = slow.next, fast.next.next

        prev, curr = None, slow.next
        slow.next = None

        while curr:
            curr.next, prev, curr = prev, curr, curr.next

        first, second = head, prev
        while second:
            first.next, first = second, first.next
            second.next, second = first, second.next
        
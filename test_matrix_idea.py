import numpy as np
from typing import List, Set, Dict


class FollowerGraph:
    def __init__(self):
        self.user_to_index: Dict[str, int] = {}
        self.index_to_user: Dict[int, str] = {}
        self.next_index: int = 0
        self.adjacency_matrix: np.ndarray = np.zeros((0, 0), dtype=np.int8)

    def add_follow(self, follower: str, following: str) -> None:
        """Add a follow relationship to the graph."""
        # Ensure both users are in our mappings
        for user in [follower, following]:
            if user not in self.user_to_index:
                self.user_to_index[user] = self.next_index
                self.index_to_user[self.next_index] = user
                self.next_index += 1

                # Expand matrix if needed
                if self.adjacency_matrix.shape[0] < self.next_index:
                    new_size = (self.next_index, self.next_index)
                    new_matrix = np.zeros(new_size, dtype=np.int8)
                    new_matrix[:self.adjacency_matrix.shape[0], :self.adjacency_matrix.shape[1]] = self.adjacency_matrix
                    self.adjacency_matrix = new_matrix

        # Set the follow relationship in the matrix
        follower_idx = self.user_to_index[follower]
        following_idx = self.user_to_index[following]
        self.adjacency_matrix[follower_idx, following_idx] = 1

    def print_matrix_state(self, vector, iteration):
        """Helper to print current state in a readable format"""
        print(f"\n=== Iteration {iteration} ===")

        # Print the current vector with user labels
        print("\nCurrent vector (who we can reach):")
        for idx, value in enumerate(vector):
            if value > 0:
                print(f"{self.index_to_user[idx]}: {value}")

        # Print the adjacency matrix with labels
        print("\nAdjacency matrix (who follows whom):")
        print("        ", end="")
        for i in range(self.next_index):
            print(f"{self.index_to_user[i]:4}", end=" ")
        print()

        for i in range(self.next_index):
            print(f"{self.index_to_user[i]:4}", end=" ")
            for j in range(self.next_index):
                print(f"{self.adjacency_matrix[i][j]:4}", end=" ")
            print()

    def get_n_degree_followers(self, user: str, n: int) -> Set[str]:
        """
        Get all n-degree followers for a user using matrix multiplication.
        Returns a set of usernames that are reachable in exactly n steps.
        """
        if user not in self.user_to_index:
            return set()

        user_idx = self.user_to_index[user]

        # Create a vector with 1 at the user's index
        start_vector = np.zeros(self.next_index, dtype=np.int8)
        start_vector[user_idx] = 1

        print("\nInitial state:")
        self.print_matrix_state(start_vector, 0)

        # Multiply by adjacency matrix n times
        result_vector = start_vector
        for i in range(n):
            result_vector = result_vector @ self.adjacency_matrix
            self.print_matrix_state(result_vector, i + 1)

        # Convert result back to usernames
        following_indices = np.nonzero(result_vector)[0]
        return {self.index_to_user[idx] for idx in following_indices}


def test_follower_graph():
    print("Creating test graph:")
    print("A -> B -> D")
    print("A -> C -> E")

    graph = FollowerGraph()

    # Build test graph
    graph.add_follow("A", "B")
    graph.add_follow("A", "C")
    graph.add_follow("B", "D")
    graph.add_follow("C", "E")

    print("\nTesting 2-degree followers for A...")
    followers = graph.get_n_degree_followers("A", 2)
    print(f"\nFinal result (2-degree followers of A): {followers}")


if __name__ == "__main__":
    test_follower_graph()
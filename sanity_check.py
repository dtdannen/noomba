import numpy as np


def print_matrix(matrix, label=None, width=4):
    """Print a matrix with aligned columns and optional label."""
    if label:
        print(f"\n{label}:")

    # Print header (A, B, C, D, E)
    print(" " * width, end=" ")
    for i in range(matrix.shape[1]):
        print(f"{chr(65 + i):>{width}}", end=" ")
    print()

    # Print each row
    for i in range(matrix.shape[0]):
        print(f"{chr(65 + i):>{width}}", end=" ")
        for j in range(matrix.shape[1]):
            print(f"{matrix[i, j]:>{width}}", end=" ")
        print()


def print_multiplication_step(adj_matrix, identity_vector, result_vector, step):
    """Print one step of matrix multiplication in a row format."""
    print(f"\n=== Step {step} ===")

    # Calculate width needed for each matrix
    width = 4
    total_width = (width + 1) * adj_matrix.shape[1] + width

    # Print adjacency matrix
    print_matrix(adj_matrix, "Original adjacency matrix")

    # Print identity vector
    print_matrix(identity_vector.reshape(-1, 1), "\nCurrent state vector")

    # Print result vector
    print_matrix(result_vector.reshape(-1, 1), "\nResult vector")

    print("\nIn other words:")
    # Print who can be reached at this step
    reached = [chr(65 + i) for i, val in enumerate(result_vector) if val > 0]
    if reached:
        print(f"We can reach: {', '.join(reached)}")
    else:
        print("We can't reach anyone new at this step")


def main():
    # Create 5x5 adjacency matrix for A->E
    adj_matrix = np.array([
        [0, 1, 1, 0, 0],  # A follows B and C
        [0, 0, 0, 1, 0],  # B follows D
        [0, 0, 0, 0, 1],  # C follows E
        [0, 0, 0, 0, 0],  # D follows nobody
        [0, 0, 0, 0, 0]  # E follows nobody
    ])

    print("\nGraph structure:")
    print("A -> B -> D")
    print("A -> C -> E")

    # Starting from A (index 0)
    current_vector = np.array([1, 0, 0, 0, 0])

    print("\nStarting from A, let's see who we can reach at each step...")

    # Perform multiplication steps
    for step in range(3):  # Show 3 steps
        print_multiplication_step(
            adj_matrix,
            current_vector,
            current_vector @ adj_matrix,
            step + 1
        )
        current_vector = current_vector @ adj_matrix


if __name__ == "__main__":
    main()
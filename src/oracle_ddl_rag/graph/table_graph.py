"""Graph-based table relationship navigation using NetworkX."""

from typing import Optional
from dataclasses import dataclass
import networkx as nx


@dataclass
class JoinStep:
    """A single step in a join path."""
    step_number: int
    from_table: str
    to_table: str
    join_condition: str
    constraint_name: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "step": self.step_number,
            "from_table": self.from_table,
            "to_table": self.to_table,
            "join_condition": self.join_condition,
            "constraint_name": self.constraint_name,
        }


@dataclass
class JoinPath:
    """Complete join path between two tables."""
    source: str
    target: str
    steps: list[JoinStep]
    total_hops: int

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "total_hops": self.total_hops,
            "steps": [s.to_dict() for s in self.steps],
        }

    def to_sql(self) -> str:
        """Generate SQL JOIN clause."""
        if not self.steps:
            return ""

        joins = []
        for step in self.steps:
            joins.append(f"JOIN {step.to_table} ON {step.join_condition}")
        return "\n".join(joins)


class TableGraph:
    """Graph for navigating table relationships via FK constraints."""

    def __init__(self):
        """Initialize an empty graph."""
        # Undirected graph (JOINs work both ways)
        self.graph = nx.Graph()

    def add_relationship(
        self,
        parent_table: str,
        child_table: str,
        parent_columns: list[str],
        child_columns: list[str],
        constraint_name: Optional[str] = None,
    ) -> None:
        """Add a FK relationship to the graph.

        Args:
            parent_table: Parent (referenced) table name.
            child_table: Child (referencing) table name.
            parent_columns: Parent table columns in the FK.
            child_columns: Child table columns in the FK.
            constraint_name: Optional FK constraint name.
        """
        parent_table = parent_table.upper()
        child_table = child_table.upper()

        # Store both directions with column info
        self.graph.add_edge(
            parent_table,
            child_table,
            parent_columns=parent_columns,
            child_columns=child_columns,
            constraint_name=constraint_name,
            parent=parent_table,
            child=child_table,
        )

    def build_from_relationships(self, relationships: list[dict]) -> None:
        """Build graph from a list of relationship dictionaries.

        Args:
            relationships: List of dicts with parent_table, child_table,
                          parent_columns, child_columns, constraint_name.
        """
        for rel in relationships:
            self.add_relationship(
                parent_table=rel["parent_table"],
                child_table=rel["child_table"],
                parent_columns=rel.get("parent_columns", []),
                child_columns=rel.get("child_columns", []),
                constraint_name=rel.get("constraint_name"),
            )

    def find_shortest_path(
        self,
        source: str,
        target: str,
        max_hops: int = 4,
    ) -> Optional[JoinPath]:
        """Find the shortest join path between two tables.

        Args:
            source: Starting table name.
            target: Target table name.
            max_hops: Maximum number of intermediate tables.

        Returns:
            JoinPath object or None if no path exists.
        """
        source = source.upper()
        target = target.upper()

        if source not in self.graph:
            return None
        if target not in self.graph:
            return None

        try:
            path = nx.shortest_path(self.graph, source, target)
        except nx.NetworkXNoPath:
            return None

        # Check max hops (path length - 1 = number of edges)
        if len(path) - 1 > max_hops:
            return None

        # Build join steps
        steps = []
        for i in range(len(path) - 1):
            from_table = path[i]
            to_table = path[i + 1]

            edge_data = self.graph.edges[from_table, to_table]
            join_condition = self._format_join_condition(
                from_table, to_table, edge_data
            )

            steps.append(JoinStep(
                step_number=i + 1,
                from_table=from_table,
                to_table=to_table,
                join_condition=join_condition,
                constraint_name=edge_data.get("constraint_name"),
            ))

        return JoinPath(
            source=source,
            target=target,
            steps=steps,
            total_hops=len(steps),
        )

    def _format_join_condition(
        self,
        from_table: str,
        to_table: str,
        edge_data: dict,
    ) -> str:
        """Format the JOIN condition between two tables.

        Args:
            from_table: First table in the join.
            to_table: Second table in the join.
            edge_data: Edge metadata with column mappings.

        Returns:
            SQL JOIN condition string.
        """
        parent = edge_data.get("parent")
        child = edge_data.get("child")
        parent_cols = edge_data.get("parent_columns", [])
        child_cols = edge_data.get("child_columns", [])

        conditions = []
        for pc, cc in zip(parent_cols, child_cols):
            if from_table == parent:
                conditions.append(f"{parent}.{pc} = {child}.{cc}")
            else:
                conditions.append(f"{child}.{cc} = {parent}.{pc}")

        return " AND ".join(conditions) if conditions else f"{from_table}.id = {to_table}.id"

    def get_direct_relationship(
        self,
        table_a: str,
        table_b: str,
    ) -> Optional[dict]:
        """Get direct FK relationship between two tables.

        Args:
            table_a: First table name.
            table_b: Second table name.

        Returns:
            Relationship dict or None if not directly connected.
        """
        table_a = table_a.upper()
        table_b = table_b.upper()

        if not self.graph.has_edge(table_a, table_b):
            return None

        edge_data = self.graph.edges[table_a, table_b]
        parent = edge_data.get("parent")
        child = edge_data.get("child")

        return {
            "parent_table": parent,
            "child_table": child,
            "parent_columns": edge_data.get("parent_columns", []),
            "child_columns": edge_data.get("child_columns", []),
            "constraint_name": edge_data.get("constraint_name"),
            "join_condition": self._format_join_condition(
                table_a, table_b, edge_data
            ),
        }

    def get_related_tables(
        self,
        table_name: str,
        max_hops: int = 2,
    ) -> list[dict]:
        """Get all tables within N hops of a given table.

        Args:
            table_name: Starting table name.
            max_hops: Maximum distance.

        Returns:
            List of dicts with table_name and distance.
        """
        table_name = table_name.upper()

        if table_name not in self.graph:
            return []

        related = []
        lengths = nx.single_source_shortest_path_length(
            self.graph, table_name, cutoff=max_hops
        )

        for other_table, distance in lengths.items():
            if other_table != table_name:
                related.append({
                    "table_name": other_table,
                    "distance": distance,
                })

        # Sort by distance
        related.sort(key=lambda x: (x["distance"], x["table_name"]))
        return related

    def get_all_tables(self) -> list[str]:
        """Get all table names in the graph."""
        return list(self.graph.nodes())

    def get_stats(self) -> dict:
        """Get graph statistics."""
        return {
            "tables": self.graph.number_of_nodes(),
            "relationships": self.graph.number_of_edges(),
        }

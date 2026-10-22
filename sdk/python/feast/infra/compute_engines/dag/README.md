# DAG
Directed Acyclic Graph (DAG) representation for feature engineering workflows in Feast. This module is designed to remain abstracted and independent of Feast feature view business logic.

Core concepts include:
- [context](./context.py): ExecutionContext for managing feature view execution.
- [node](./node.py): DAGNode for representing operations in the DAG.
- [value](./value.py): DAGValue class for handling data passed between nodes.
- [model](./model.py): Model class for defining the dag data classes such as DAGFormat.
- [plan](./plan.py): ExecutionPlan for executing the DAG.

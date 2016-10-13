# [It's Roasting in Here!](http://nestedtori.tech)
Insight Data Engineering Project: Streaming Heat Generation Data and Solving the Heat Equation

---
## Table of Contents
1. [Introduction](#intro)
2. Diffusion as a Stateful Streaming Problem
3. Cluster Structure
4. Pipeline
5. Presentation
6. Scaling and Performance

---

## <a name="intro"></a> Introduction
The process of diffusion is a well-known one in the physical sciences, and has been used to successfully model the spread of many things that are not necessarily motivated by physical processes (we'll see some examples when we discuss what kind of problem this really turns into). For this project, however, we'll focus on a venerable and favorite diffusion problem, that of heat propagation. We show how this type of problem can be recast as a very common streaming problem, that of stateful streaming.

### Mathematical Description: the 2D Heat Equation
This section is optional; it describes the underlying mathematical derivation of the equation we'll present in the next section. We consider the heat equation for temperature _u_ of a continuous system in a planar domain. Here, $u = u(x,y,t)$, a function of two space coordinates and time, measured in, say $^\circ$F or $^\circ$C. The equation is:

$$\dfrac{\partial u}{\partial t} = k\ \Delta u + f$$

where $k$ is a constant called the _conductivity_, the $\Delta$ is the 2D Laplace operator

$$\frac{\partial^2}{\partial x^2} + \frac{\partial^2 }{\partial y^2},$$

and $f$ is the rate of heat generation at each point in the domain (measured in something like calories per second, or some other relevant proxy, such as number of instructions per second–you'll need an extra constant in that case). Intuitively, this means the time rate of change of the temperature is proportional to the (negative) of how the heat is concentrated. To all this, we add the heat generation data. The purpose of the $\Delta$ operator is to capture the fact that heat _diffuses_ throughout space as time passes, thus making the problem more complicated than just working with a single rate of change and integrating (which would be an _ordinary_ differential equation rather than _partial_). Even if there's _no_ input (or if the input suddenly cuts off), $u(x,y,t)$ continues to change as heat spreads and dissipates. We have to add boundary conditions, which is what happens at the boundaries of the region. For this probem, we choose the boundary condition of $u = 0$ (called "Dirichlet conditions"), which represents a heat sink on all boundaries.

To solve this numerically, we have to discretize in both time and space. For the spatial part, we'll assume it's a bunch of rectangular grid points as follows:

![rectangular grid](images/grid)

$u$ is now represented as a matrix $(u_{i,j})$  with each entry corresponding to the grid; in common programming languages, it looks like `u(i,j)` (MATLAB-like languages) or `u[i][j]` (C-like languages), or just some positional function like `u.at(i,j)`. The operator $\Delta$ takes the form of a centered difference, using the points in a _stencil_: the use of the 4 nearest neighbors along with the central points:

![stencil](images/stencil)

For a grid size $h$ we add the value of $u$ at the point to the left and right, and the value of $u$ at the point above and below, and finally subtract four times the middle, and finally divide by the square of the grid spacing $h$.

In formulas, $\Delta_h u$ at the point $i,j$ is:

$$(\Delta_h u)_{i,j} = \frac{u_{i-1,j} + u_{i+1,j} - 4u_{i,j} + u_{i,j-1} + u_{i,j+1}}{h^2}$$


### Algorithm
We have so far derived the relation

$$u^{n+1}_{i,j} = u^n_{i,j} - k \, \frac{\tau}{h^2}\, \left(u^n_{i-1,j} + u^n_{i+1,j} - 4u^n_{i,j} + u^n_{i,j-1} + u^n_{i,j+1} + f_{i,j}\right).$$

It is convenient to combine all the constants in one (we'll call it $C$). Many mathematicians prefer "nondimensionalization" where they set all constants to 1, but it is at the cost of having to figure out exactly what simulation parameters the equation actually represents. So we'll at least keep $C$ around.

Let's translate this into a form suitable to programming in a streaming framework. First, noting that the superscript _n_ is for time, we see that it only depends on one previous time step, so that we can conceive of this as updating one variable (actually dependence on multiple timesteps can be modeled as a system two variables, so this actually is not a loss of generality). Here by "variable" we actually mean a full 2D array varaible,. In traditional programming applications, states are precisely what is modeled by variables. In streaming applications, state variables are stored separately in a cache-type database, such as RocksDB.

## <a name="statestream"></a> Diffusion as a Stateful Streaming Problem

(Coming soon)

## <a name="cluster"> </a> Cluster Structure
The structure of the clustes used:
1. Four `m4.large` nodes for the Kafka cluster (4 workers; leader elected)
2. Four `m4.large` nodes for the Cassandra cluster (master-slave, 1 leader, 3 workers)
3. One `t2.micro` web server.


## <a name="statestream"></a> Pipeline
The pipeline we used is as follows:
![pipeline pic](images/pipeline)

## <a name="presentation"></a> Presentation

At the [demo site](http://nestedtori.tech) we are presented with two options: `roasting_ui_b_str` for the streaming version, and `roasting_ui_b` for the batch version. As we'll note in the [Performance](#performance) section above, the batch version is the much better-looking and more correct version, but it is much slower; it took around 90 minutes to generate 5 minutes of data (where the events are happening at about 10 per second, per node). 

## <a name="scaling"></a> Scaling and Performance

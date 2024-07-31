# Language

## Python: The Language of Production Machine Learning

Use Python to serve your features online.


## Why should you use Python to Serve features for Machine Learning? 
Python has emerged as the primary language for machine learning, and this extends to feature serving and there are five main reasons Feast recommends using a microservice in Feast.

### 1. Python is the language of Machine Learning

You should meet your users where they are. Python’s popularity in the machine learning community is undeniable. Its simplicity and readability make it an ideal language for writing and understanding complex algorithms. Python boasts a rich ecosystem of libraries such as TensorFlow, PyTorch, XGBoost, and scikit-learn, which provide robust support for developing and deploying machine learning models and we want Feast in this ecosystem.

### 2. Precomputation is The Way

Precomputing features is the recommended optimal path to ensure low latency performance. Reducing feature serving to a lightweight database lookup is the ideal pattern, which means the marginal overhead of Python should be tolerable. Precomputation ensures product experiences for downstream services are also fast. Slow user experiences are bad user experiences. Precompute and persist data as much as you can.

### 3. Serving features in another language can lead to skew
Ensuring that features used during model training (offline serving) and online serving are available in production to make real-time predictions is critical. When features are initially developed, they are typically written in Python. This is due to the convenience and efficiency provided by Python's data manipulation libraries. However, in a production environment, there is often interest or pressure to rewrite these features in a different language, like Java, Go, or C++, for performance reasons. This reimplementation introduces a significant risk: training and serving skew. Note that there will always be some minor exceptions (e.g., any *Time Since Last Event* types of features) but this should not be the rule.

Training and serving skew occurs when there are discrepancies between the features used during model training and those used during prediction. This can lead to degraded model performance, unreliable predictions, and reduced velocity in releasing new features and new models. The process of rewriting features in another language is prone to errors and inconsistencies, which exacerbate this issue.

### 4. Reimplementation is Excessive 

Rewriting features in another language is not only risky but also resource-intensive. It requires significant time and effort from engineers to ensure that the features are correctly translated. This process can introduce bugs and inconsistencies, further increasing the risk of training and serving skew. Additionally, maintaining two versions of the same feature codebase adds unnecessary complexity and overhead. More importantly, the opportunity cost of this work is high and requires twice the amount of resourcing. Reimplementing code should only be done when the performance gains are worth the investment. Features should largely be precomputed so the latency performance gains should not be the highest impact work that your team can accomplish.

### 5. Use existing Python Optimizations

Rather than switching languages, it is more efficient to optimize the performance of your feature store while keeping Python as the primary language. Optimization is a two step process.

#### Step 1: Quantify latency bottlenecks in your feature calculations

Use tools like [CProfile](https://docs.python.org/3/library/profile.html) to understand latency bottlenecks in your code. This will help you prioritize the biggest inefficiencies first. When we initially launched Python native transformations in Python, [profiling the code](https://github.com/feast-dev/feast/issues/4207#issuecomment-2155754504) helped us identify that Pandas resulted in a 10x overhead due to type conversion.

#### Step 2: Optimize your feature calculations

As mentioned, precomputation is the recommended path. In some cases, you may want fully synchronous writes from your data producer to your online feature store, in which case you will want your feature computations and writes to be very fast. In this case, we recommend optimizing the feature calculation code first. 

You should optimize your code using libraries, tools, and caching. For example, identify whether your feature calculations can be optimized through vectorized calculations in NumPy; explore tools like Numba for faster execution; and cache frequently accessed data using tools like an lru_cache.

Lastly, Feast will continue to optimize serving in Python and making the overall infrastructure more performant. This will better serve the community.

So we recommend focusing on optimizing your feature-specific code, reporting latency bottlenecks to the maintainers, and contributing to help the infrastructure be more performant.

By keeping features in Python and optimizing performance, you can ensure consistency between training and serving, reduce the risk of errors, and focus on launching more product experiences for your customers. 

Embrace Python for feature serving, and leverage its strengths to build robust and reliable machine learning systems.

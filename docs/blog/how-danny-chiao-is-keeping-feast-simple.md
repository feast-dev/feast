# How Danny Chiao is Keeping Feast Simple

*March 2, 2022* | *Claire Besset*

Tecton Engineer Danny Chiao recently appeared on *The Feast Podcast* to have a conversation with host Demetrios Brinkmann, head of the [MLOps Community](https://mlops.community). Demetrios and Danny spent an hour together discussing why Danny left Google to work on Feast, what it's like to be a leader in an open-source community, and what the future holds for Feast and MLOps. You can read about the highlights from their conversation below, or listen to the full episode [here](https://anchor.fm/featurestore).

## From Google to Feast

Prior to joining Tecton, Danny spent 7.5 years at Google, working on everything from Google+ to Android to Google Workspace. As a machine learning engineer, he worked with stakeholders from both product and research teams. Bridging gaps between these teams was a two-way challenge: product teams needed help applying learnings from research teams, and research teams needed to be convinced to take on projects from the product space.

In addition, it was difficult to share data from enterprise Google products with research teams due to security and privacy mandates. Danny's experience working on multiple ML products and interfacing between diverse stakeholder groups would later prove to be highly valuable in his role in the Feast open source community.

What prompted Danny to leave Google and join Tecton? He noticed how the ML landscape outside of Google was starting to look very different from how it did internally. While Google was still using ETL jobs to read data from data lakes or databases and perform massive transformations, other companies were taking advantage of new data warehouse technologies: "I was hearing that the ecosystem for iterating, developing, and shipping models was oddly enough more mature outside of Google…Internally, a lot of these massive systems are dependent on the infrastructure, so you can't iterate as quickly."

Excited by the innovations in ML infrastructure that were appearing in the broader community, Danny moved to Tecton to work on [Feast](https://www.tecton.ai/blog/feast-announcement/), an open-source feature store. [Feature stores](https://www.tecton.ai/blog/what-is-a-feature-store/) act as a central hub for feature data across an ML project's lifecycle, and are responsible for transforming raw data into features, storing and managing features, and serving features for training and prediction. Feature stores are quickly becoming a critical infrastructure for data science teams putting ML into production.

## What it's like to work in the Feast open source community

As a leader in the Feast community, Danny splits his time between engineering projects and community engagement. In working with the community, Danny is learning about the current and emerging use cases for Feast. One of the big challenges with Feast is its broad user-base: "We have users coming to us like, 'Hey, I don't have that much data. I don't have super-strict latency requirements. I don't need a lot of complexity.' Then you have the Twitters of the world who are like, 'Hey, we need massive scale, massive low latency.' There's definitely that tug."

There are also diverse usecases for Feast, from recommender systems, to fraud detection, to credit scoring, to biotech. The solution has been to keep Feast as simple and streamlined as possible. It should be flexible and extensible enough to meet the needs of its broad community, but it also aims to be accessible for small companies just beginning machine learning operations. As Danny says, "You can't get all these new users to come in and enjoy value if it's going to take a really, really long time to stand something up."

This was the vision behind the release of Feast 0.10, which is Python-centric and can run on a developer's local machine. Overall, Danny holds a very positive outlook on the future of collaboration within Feast, noting how the diversity of the community can be an asset: "If you can motivate the right people and drive people towards the same vision, then you can do things way faster than if you were just a small team executing on it."

## The future for Feast

What's on the docket for Feast development this year? They're working with companies like Twitter and Redis to get benchmarks on how performant Feast is and harden the serving layer. Danny's excited to work on data quality monitoring and make that practice more standardized in the community. He's also looking forward to the launch of the Feast Web UI, because users have been asking for easier ways to discover and share features and data pipelines.

True to the vision of keeping Feast simple, the team is focused on targeting new users in the ML space and getting them from zero-to-one. This is the plan for a world where machine learning is becoming even more ubiquitous. "It's going to become something that is just expected of companies," Demetrios said. "Right now, it doesn't feel like we've even gotten at 2% of what is potentially possible if every single business is going to be using machine learning." Fortunately, feature stores are a technology that can dramatically shorten the time it takes a new company to begin realizing value from machine learning.

From meeting the machine learning needs of a broad user base to helping new teams get started with ML, there's a lot of exciting work to be done at Feast! You can learn more about the Feast project on our [website](https://www.tecton.ai/feast/), or read updates in Danny's community newsletter on the [Feast google group](https://groups.google.com/g/feast-dev/).

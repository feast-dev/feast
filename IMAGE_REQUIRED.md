# Image File Required for Blog Post

## Blog Post: Feature Transformation Latency

The blog post "Faster On Demand Transformations in Feast" has been successfully created at:
`/home/runner/work/feast/feast/infra/website/docs/blog/feature-transformation-latency.md`

However, the latency comparison image needs to be manually added:

### Required Action:

1. **Download the image** from: https://github.com/user-attachments/assets/d19a5cb6-5d5e-4993-a528-72ffaaebb925

2. **Save it as**: `/home/runner/work/feast/feast/infra/website/public/images/blog/latency-comparison.png`

### Image Description:
The image shows a side-by-side comparison of:
- **Left**: Pandas Latency Decomposition
- **Right**: Native Python Latency Decomposition

It demonstrates that Native Python transformations are approximately 10x faster than Pandas transformations.

### Note:
The blog post references this image in the hero section. Without this image, the blog post will still render but the image will show as a broken link.

Once the image is added, the website can be rebuilt with:
```bash
cd /home/runner/work/feast/feast/infra/website
npm run build
```

---
description: 'Tracking Feast''s API Compatibility, support Feast versions, Deprecation.'
---

# API, Supported Versions & Deprecation

## Overview

This document tracks Feast's API compatibility. Users are dependent on Feast providing a stable, compatible API such that Feast upgrades do not significantly impact the systems that they have built on Feast, while developers have to make changes to correct API design decisions that no longer make sense. By tracking Feast's API Compatibility, support Feast versions, this document attempts to find common ground between the two needs.

1. [Release Versioning](api-supported-versions-and-deprecation.md#1-release-versioning)
2. [API Compatibility Policy](api-supported-versions-and-deprecation.md#2-api-compatibility)
3. [Supported Feast Versions.](api-supported-versions-and-deprecation.md#3-supported-versions)
4. [Deprecations.](api-supported-versions-and-deprecation.md#4-deprecations)

## 1. Release Versioning

Feast follows [semantic versioning ](https://semver.org/)to version its components in the form `MAJOR.MINOR.PATCH`. Different Feast Components are versioned together \(ie Core, Serving, SDKs\). 

* Stable releases are versioned `MAJOR.MINOR.PATCH`
* Pre-release releases are versioned as `MAJOR.MINOR.PATCH-SNAPSHOT` or `MAJOR.MINOR.PATCH-rc.NUMBER` for release candidates.

{% hint style="danger" %}
While Feast is still pre-1.0 breaking changes will happen in minor versions.
{% endhint %}

## 2. API Compatibility

### Defining API

Feast defines API as anything that is directly user facing, accessible by the user directly. This includes:

* SDKs' user facing methods/classes.
* Protobufs used in Feast messaging.
* gRPC/HTTP Service APIs.
* Database schemas.
* Configuration Files.

### Breaking Changes

Defining Breaking Changes:

* Adding something that users need to use for existing functionality \(eg. adding a required parameter\).
* Changing how a given API should be interpreted.
* Changing a default value in parameter in the API.
* Removing something from the API.

### Introducing Breaking Changes

What steps should developers take when proposing breaking changes.

* Propose a deprecation by making a Pull Request and updating the `Deprecation` section in this document.
* Flag deprecated APIs with `deprecated` in code and in release notes in the Pull Request.

Once deprecation period has expired the developer may introduce the breaking change:

* Flag Breaking changes in release notes with `breaking` and `action required`.
* Ensure that **detailed upgrade instructions** are provided in the release notes for users to follow when upgrading.

## 3. Supported Versions

### Support Table

Support Table defines which versions of Feast components are currently supported:

| Component | Supported Versions |
| :--- | :--- |
| Feast Core | 0.7 |
| Feast Serving | 0.7 |
| Feast Go SDK | 0.6, 0.7 |
| Feast Python SDK/CLI | 0.6, 0.7 |
| Feast Java SDK | 0.6, 0.7 |

### Patch Releases

Patch Releases typically contain critical backwards compatible bug fixes. Users should ensure that they are running the latest patch release of the selected minor version of Feast.

### **Component Skew**

Feast's toleration for Component Skew is as follows:

* Feast Core, Serving, Job Coordinator are compatible with the same patch version.
* Feast Core/Serving/Job Coordinator are backwards compatible with Feast SDKs for one minor version. \(ie Feast 0.6 is compatible with Feast 0.5 SDKs\).

## 4. Deprecations

Tracks deprecation in Feast APIs, expiry release and mitigation and migration. 

{% hint style="warning" %}
A breaking change or removal can be made to a deprecated API when it reaches its expiry release. Users are encouraged to migrate their systems before the expiry release.
{% endhint %}

<table>
  <thead>
    <tr>
      <th style="text-align:left">Deprecation</th>
      <th style="text-align:left">Breaking Change Release</th>
      <th style="text-align:left">Components Affected</th>
      <th style="text-align:left">Mitigation/Migration</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">Specifying project in <a href="https://github.com/feast-dev/feast/blob/5b72335ca08dad361895fc928918615cd63b2158/protos/feast/serving/ServingService.proto#L83">FeatureReference</a>s
        when retrieving from Serving via <code>GetOnlineFeatures </code>is no longer
        supported.</td>
      <td style="text-align:left">0.8</td>
      <td style="text-align:left">Serving&apos;s Service API</td>
      <td style="text-align:left">
        <ul>
          <li>SDK users: Migrate to a SDK with version &gt;= 0.6</li>
          <li>API users: Use specialized <a href="https://github.com/feast-dev/feast/blob/5b72335ca08dad361895fc928918615cd63b2158/protos/feast/serving/ServingService.proto#L97"><code>project</code></a> field
            in <code>GetOnlineFeaturesRequest</code> when retrieving via <code>GetOnlineFeatures</code>.</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left">
        <ul>
          <li><code>get_batch_features</code> will be changed to <code>get_historical_features</code>
          </li>
          <li><code>get_online_features</code> entity_rows field will only take in a
            list of dictionaries instead of<code>GetOnlineFeaturesRequest.EntityRow</code>
          </li>
        </ul>
      </td>
      <td style="text-align:left">0.8</td>
      <td style="text-align:left">Python SDK</td>
      <td style="text-align:left">
        <ul>
          <li>Python SDK users: Migrate to a SDK with version &gt;= 0.7</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>


<p align="center">
    <a href="https://feast.dev/">
      <img src="docs/assets/feast_logo.png" width="550">
    </a>
</p>
<br />

[![Unit Tests](https://github.com/feast-dev/feast/workflows/unit%20tests/badge.svg?branch=master)](https://github.com/feast-dev/feast/actions?query=workflow%3A%22unit+tests%22+branch%3Amaster)
[![Code Standards](https://github.com/feast-dev/feast/workflows/code%20standards/badge.svg?branch=master)](https://github.com/feast-dev/feast/actions?query=workflow%3A%22code+standards%22+branch%3Amaster)
[![Docs Latest](https://img.shields.io/badge/docs-latest-blue.svg)](https://docs.feast.dev/)
[![GitHub Release](https://img.shields.io/github/v/release/feast-dev/feast.svg?style=flat&sort=semver&color=blue)](https://github.com/feast-dev/feast/releases)

## Overview

Feast (Feature Store) is an operational data system for managing and serving machine learning features to models in production. Please see our [documentation](https://docs.feast.dev/) for more information about the project.

![](docs/.gitbook/assets/feast-architecture-diagrams.svg)

## Getting Started with Docker Compose

Clone the latest stable version of the [Feast repository](https://github.com/feast-dev/feast/) and navigate to the `infra/docker-compose` sub-directory:

```
git clone https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
```

The `.env` file can optionally be configured based on your environment.

Bring up Feast:
```
docker-compose pull && docker-compose up -d
```
Please wait for the containers to start up. This could take a few minutes since the quickstart contains demo infastructure like Kafka and Jupyter.

Once the containers are all running, please connect to the provided [Jupyter Notebook](http://localhost:8888/tree/minimal) containing example notebooks to try out.

## Important resources

Please refer to the official documentation at <https://docs.feast.dev>

 * [Concepts](https://docs.feast.dev/concepts/overview)
 * [Installation](https://docs.feast.dev/getting-started)
 * [Examples](https://github.com/feast-dev/feast/blob/master/examples/)
 * [Roadmap](https://docs.feast.dev/roadmap)
 * [Change Log](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md)
 * [Slack (#Feast)](https://join.slack.com/t/tectonfeast/shared_invite/zt-n7pl8gnb-H7dLlH9yQsgbchOp36ZUxQ)

## Notice

Feast is a community project and is still under active development. Your feedback and contributions are important to us. Please have a look at our [contributing guide](https://docs.feast.dev/contributing/contributing) for details.

## Contributors ✨

Thanks goes to these incredible people:

<table>
  <tr>
    <td align="center"><a href="https://github.com/budi"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/252022?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="http://investors.avanza.se/en"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/161591?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/accraze"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/989447?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://ankurs.com/"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/7549?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/AnujaVane"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/56522303?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/ashwinath"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/13537118?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/baskaranz"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/6318819?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/zhilingc"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/15104168?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="http://chesmart.in/"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/13277?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/davidheryanto"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/5300554?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="http://www.enginpolat.com/"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/118744?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/gauravkumar37"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/2500570?v=4" width="50px;" alt=""/></a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/gabrielwen"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/4784270?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://iain.rauch.co.uk/"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/6860163?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/jmelinav"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/25539467?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/Jeffwan"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/4739316?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/Joostrothweiler"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/7423624?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://imjuanleonard.com/"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/7872644?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/khorshuheng"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/32997938?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/lavkesh"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/893339?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/lgvital"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/523921?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/mansiib"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/21190165?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/MichaelHirn"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/3092059?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/mike0sv"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/5938179?v=4" width="50px;" alt=""/></a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/oavdeev"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/3689?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/pyalex"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/1303659?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/peterjrichens"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/22096708?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="http://ravisuhag.com/"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/2075279?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/romanwozniak"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/1886194?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/SwampertX"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/17807016?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://medium.com/@terencelimxp"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/25025366?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/duongnt"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/759564?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://tims.codes/"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/63295?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/tsotnet"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/5042959?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/woop"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/6728866?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/thirteen37"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/1531839?v=4" width="50px;" alt=""/></a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://www.linkedin.com/in/zhu-zhanyan/"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/15938899?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/chengcheng-pei"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/57113014?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/apps/dependabot"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/in/29110?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/dr3s"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/145115?v=4" width="50px;" alt=""/></a></td>    
    <td align="center"><a href="https://github.com/junhui096"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/35248886?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/oavdeev-tt"><img style="border-radius: 25px;" src="https://avatars2.githubusercontent.com/u/58185307?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/phadthai"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/73770010?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/pradithya"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/4023015?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/smadarasmi"><img style="border-radius: 25px;" src="https://avatars0.githubusercontent.com/u/23423749?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/suwik"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/10407345?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/voonhous"><img style="border-radius: 25px;" src="https://avatars1.githubusercontent.com/u/6312314?v=4" width="50px;" alt=""/></a></td>
    <td align="center"><a href="https://github.com/david30907d"><img style="border-radius: 25px;" src="https://avatars3.githubusercontent.com/u/9366404?v=4" width="50px;" alt=""/></a></td>
  </tr>
</table>

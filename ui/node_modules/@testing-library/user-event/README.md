<div align="center">
<h1>user-event</h1>

<a href="https://www.joypixels.com/profiles/emoji/1f415">
  <img
    height="80"
    width="80"
    alt="dog"
    src="https://raw.githubusercontent.com/testing-library/user-event/main/other/dog.png"
  />
</a>

<p>Fire events the same way the user does</p>

<br />

[**Read The Docs**](https://testing-library.com/docs/ecosystem-user-event) |
[Edit the docs](https://github.com/testing-library/testing-library-docs)

<br />
</div>

---

<!-- prettier-ignore-start -->
[![Build Status][build-badge]][build]
[![Code Coverage][coverage-badge]][coverage]
[![version][version-badge]][package]
[![downloads][downloads-badge]][npmtrends]
[![MIT License][license-badge]][license]
[![All Contributors][all-contributors-badge]](#contributors)
[![PRs Welcome][prs-badge]][prs]
[![Code of Conduct][coc-badge]][coc]
[![Discord][discord-badge]][discord]

[![Watch on GitHub][github-watch-badge]][github-watch]
[![Star on GitHub][github-star-badge]][github-star]
[![Tweet][twitter-badge]][twitter]
<!-- prettier-ignore-end -->

## Table of Contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [The problem](#the-problem)
- [The solution](#the-solution)
- [Installation](#installation)
- [Docs](#docs)
- [Known limitations](#known-limitations)
- [Issues](#issues)
  - [🐛 Bugs](#-bugs)
  - [💡 Feature Requests](#-feature-requests)
  - [❓ Questions](#-questions)
- [Contributors](#contributors)
- [LICENSE](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## The problem

From
[testing-library/dom-testing-library#107](https://github.com/testing-library/dom-testing-library/issues/107):

> [...] it is becoming apparent the need to express user actions on a web page
> using a higher-level abstraction than [`fireEvent`][fire-event]

## The solution

`user-event` tries to simulate the real events that would happen in the browser
as the user interacts with it. For example `userEvent.click(checkbox)` would
change the state of the checkbox.

> [The more your tests resemble the way your software is used, the more
> confidence they can give you.][guiding-principle]

## Installation

With NPM:

```sh
npm install --save-dev @testing-library/user-event @testing-library/dom
```

With Yarn:

```sh
yarn add --dev @testing-library/user-event @testing-library/dom
```

## Docs

[**Read The Docs**](https://testing-library.com/docs/ecosystem-user-event) |
[Edit the docs](https://github.com/testing-library/testing-library-docs)

## Known limitations

- No `<input type="color" />` support.
  [#423](https://github.com/testing-library/user-event/issues/423#issuecomment-669368863)

## Issues

Looking to contribute? Look for the [Good First Issue][good-first-issue] label.

### 🐛 Bugs

Please file an issue for bugs, missing documentation, or unexpected behavior.

[**See Bugs**][bugs]

### 💡 Feature Requests

Please file an issue to suggest new features. Vote on feature requests by adding
a 👍. This helps maintainers prioritize what to work on.

[**See Feature Requests**][requests]

### ❓ Questions

For questions related to using the library, please visit a support community
instead of filing an issue on GitHub.

- [Discord][discord]
- [Stack Overflow][stackoverflow]

## Contributors

Thanks goes to these people ([emoji key][emojis]):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://twitter.com/Gpx"><img src="https://avatars0.githubusercontent.com/u/767959?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Giorgio Polvara</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3AGpx" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=Gpx" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=Gpx" title="Documentation">📖</a> <a href="#ideas-Gpx" title="Ideas, Planning, & Feedback">🤔</a> <a href="#infra-Gpx" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/testing-library/user-event/pulls?q=is%3Apr+reviewed-by%3AGpx" title="Reviewed Pull Requests">👀</a> <a href="https://github.com/testing-library/user-event/commits?author=Gpx" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/weyert"><img src="https://avatars3.githubusercontent.com/u/7049?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Weyert de Boer</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=weyert" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=weyert" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/twhitbeck"><img src="https://avatars2.githubusercontent.com/u/762471?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Tim Whitbeck</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Atwhitbeck" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=twhitbeck" title="Code">💻</a></td>
    <td align="center"><a href="https://michaeldeboey.be"><img src="https://avatars3.githubusercontent.com/u/6643991?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Michaël De Boey</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=MichaelDeBoey" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/michaellasky"><img src="https://avatars2.githubusercontent.com/u/6646599?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Michael Lasky</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=michaellasky" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=michaellasky" title="Documentation">📖</a> <a href="#ideas-michaellasky" title="Ideas, Planning, & Feedback">🤔</a></td>
    <td align="center"><a href="https://github.com/shomalgan"><img src="https://avatars0.githubusercontent.com/u/2883620?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ahmad Esmaeilzadeh</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=shomalgan" title="Documentation">📖</a></td>
    <td align="center"><a href="https://calebeby.ml"><img src="https://avatars1.githubusercontent.com/u/13206945?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Caleb Eby</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=calebeby" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/issues?q=author%3Acalebeby" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/pulls?q=is%3Apr+reviewed-by%3Acalebeby" title="Reviewed Pull Requests">👀</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://afontcu.dev"><img src="https://avatars0.githubusercontent.com/u/9197791?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Adrià Fontcuberta</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Aafontcu" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=afontcu" title="Tests">⚠️</a> <a href="https://github.com/testing-library/user-event/commits?author=afontcu" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/skywickenden"><img src="https://avatars2.githubusercontent.com/u/4930551?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sky Wickenden</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Askywickenden" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=skywickenden" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/bogdanbodnar"><img src="https://avatars2.githubusercontent.com/u/9034868?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Bodnar Bogdan</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Abogdanbodnar" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=bogdanbodnar" title="Code">💻</a></td>
    <td align="center"><a href="https://zach.website"><img src="https://avatars0.githubusercontent.com/u/1699281?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Zach Perrault</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=zperrault" title="Documentation">📖</a></td>
    <td align="center"><a href="https://twitter.com/ryanastelly"><img src="https://avatars1.githubusercontent.com/u/4138357?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ryan Stelly</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=FLGMwt" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/benmonro"><img src="https://avatars3.githubusercontent.com/u/399236?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ben Monro</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=benmonro" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/GentlemanHal"><img src="https://avatars2.githubusercontent.com/u/415521?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Christopher Martin</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=GentlemanHal" title="Code">💻</a></td>
  </tr>
  <tr>
    <td align="center"><a href="http://fullgallop.me"><img src="https://avatars0.githubusercontent.com/u/32252769?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Yuancheng Wu</b></sub></a><br /><a href="https://github.com/testing-library/user-event/pulls?q=is%3Apr+reviewed-by%3AYuanchengWu" title="Reviewed Pull Requests">👀</a></td>
    <td align="center"><a href="https://github.com/maheshjag"><img src="https://avatars0.githubusercontent.com/u/1705603?v=4?s=100" width="100px;" alt=""/><br /><sub><b>MJ</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=maheshjag" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/jmcriffey"><img src="https://avatars0.githubusercontent.com/u/2831294?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jeff McRiffey</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=jmcriffey" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=jmcriffey" title="Tests">⚠️</a></td>
    <td align="center"><a href="http://jagascript.com"><img src="https://avatars0.githubusercontent.com/u/4562878?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jaga Santagostino</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=kandros" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=kandros" title="Tests">⚠️</a></td>
    <td align="center"><a href="http://jordy.app"><img src="https://avatars3.githubusercontent.com/u/12712484?v=4?s=100" width="100px;" alt=""/><br /><sub><b>jordyvandomselaar</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=jordyvandomselaar" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=jordyvandomselaar" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://lyamkin.com"><img src="https://avatars2.githubusercontent.com/u/3854930?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ilya Lyamkin</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=ilyamkin" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=ilyamkin" title="Tests">⚠️</a></td>
    <td align="center"><a href="http://todofullstack.com"><img src="https://avatars2.githubusercontent.com/u/4474353?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Kenneth Luján Rosas</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=klujanrosas" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=klujanrosas" title="Tests">⚠️</a></td>
  </tr>
  <tr>
    <td align="center"><a href="http://thejoemorgan.com"><img src="https://avatars1.githubusercontent.com/u/2388943?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Joe Morgan</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=jsmapr1" title="Code">💻</a></td>
    <td align="center"><a href="https://twitter.com/wachunga"><img src="https://avatars0.githubusercontent.com/u/438545?v=4?s=100" width="100px;" alt=""/><br /><sub><b>David Hirtle</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=wachunga" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/bdh1011"><img src="https://avatars2.githubusercontent.com/u/8446067?v=4?s=100" width="100px;" alt=""/><br /><sub><b>whiteUnicorn</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=bdh1011" title="Code">💻</a></td>
    <td align="center"><a href="https://www.matej.snuderl.si/"><img src="https://avatars3.githubusercontent.com/u/8524109?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Matej Šnuderl</b></sub></a><br /><a href="https://github.com/testing-library/user-event/pulls?q=is%3Apr+reviewed-by%3AMeemaw" title="Reviewed Pull Requests">👀</a></td>
    <td align="center"><a href="https://pomb.us"><img src="https://avatars1.githubusercontent.com/u/1911623?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Rodrigo Pombo</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=pomber" title="Code">💻</a></td>
    <td align="center"><a href="http://github.com/Raynos"><img src="https://avatars3.githubusercontent.com/u/479538?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jake Verbaten</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=Raynos" title="Code">💻</a></td>
    <td align="center"><a href="https://skovy.dev"><img src="https://avatars1.githubusercontent.com/u/5247455?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Spencer Miskoviak</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=skovy" title="Documentation">📖</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://proling.ru/"><img src="https://avatars2.githubusercontent.com/u/16336572?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Vadim Shvetsov</b></sub></a><br /><a href="#ideas-vadimshvetsov" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/testing-library/user-event/commits?author=vadimshvetsov" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=vadimshvetsov" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/9still"><img src="https://avatars0.githubusercontent.com/u/4924760?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Greg Shtilman</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=9still" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=9still" title="Tests">⚠️</a> <a href="https://github.com/testing-library/user-event/issues?q=author%3A9still" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/rbusquet"><img src="https://avatars1.githubusercontent.com/u/7198302?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ricardo Busquet</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Arbusquet" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=rbusquet" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=rbusquet" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://www.linkedin.com/in/dougbacelar/en"><img src="https://avatars3.githubusercontent.com/u/9267678?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Doug Bacelar</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=dougbacelar" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=dougbacelar" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/kayleighridd"><img src="https://avatars3.githubusercontent.com/u/36446015?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Kayleigh Ridd</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Akayleighridd" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=kayleighridd" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=kayleighridd" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://malcolmkee.com"><img src="https://avatars0.githubusercontent.com/u/24528512?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Malcolm Kee</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=malcolm-kee" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=malcolm-kee" title="Documentation">📖</a> <a href="https://github.com/testing-library/user-event/commits?author=malcolm-kee" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/kelvinlzhang"><img src="https://avatars3.githubusercontent.com/u/8291294?v=4?s=100" width="100px;" alt=""/><br /><sub><b>kelvinlzhang</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Akelvinlzhang" title="Bug reports">🐛</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/krzysztof-hellostudio"><img src="https://avatars3.githubusercontent.com/u/1942664?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Krzysztof</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Akrzysztof-hellostudio" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/hontas"><img src="https://avatars2.githubusercontent.com/u/1521113?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Pontus Lundin</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=hontas" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=hontas" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://hudochenkov.com/"><img src="https://avatars2.githubusercontent.com/u/654597?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Aleks Hudochenkov</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Ahudochenkov" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/nanivijay"><img src="https://avatars0.githubusercontent.com/u/5945591?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Vijay Kumar Otti</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Ananivijay" title="Bug reports">🐛</a></td>
    <td align="center"><a href="http://tompicton.com"><img src="https://avatars2.githubusercontent.com/u/12588098?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Tom Picton</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Atpict" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=tpict" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=tpict" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://hung.dev"><img src="https://avatars3.githubusercontent.com/u/8603085?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Hung Viet Nguyen</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Anvh95" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://nickmccurdy.com/"><img src="https://avatars0.githubusercontent.com/u/927220?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Nick McCurdy</b></sub></a><br /><a href="#projectManagement-nickmccurdy" title="Project Management">📆</a> <a href="#question-nickmccurdy" title="Answering Questions">💬</a> <a href="https://github.com/testing-library/user-event/commits?author=nickmccurdy" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=nickmccurdy" title="Tests">⚠️</a> <a href="https://github.com/testing-library/user-event/commits?author=nickmccurdy" title="Documentation">📖</a></td>
  </tr>
  <tr>
    <td align="center"><a href="http://timdeschryver.dev"><img src="https://avatars1.githubusercontent.com/u/28659384?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Tim Deschryver</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=timdeschryver" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/ben-dyer"><img src="https://avatars2.githubusercontent.com/u/43922444?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ben Dyer</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=ben-dyer" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=ben-dyer" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://twitter.com/herecydev"><img src="https://avatars1.githubusercontent.com/u/11328618?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Dan Kirkham</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=herecydev" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/Johannesklint"><img src="https://avatars3.githubusercontent.com/u/16774845?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Johannesklint</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=Johannesklint" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/juanca"><img src="https://avatars0.githubusercontent.com/u/841084?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Juan Carlos Medina</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=juanca" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=juanca" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/WretchedDade"><img src="https://avatars0.githubusercontent.com/u/17183431?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Dade Cook</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=WretchedDade" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=WretchedDade" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://blog.lourenci.com/"><img src="https://avatars3.githubusercontent.com/u/2339362?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Leandro Lourenci</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=lourenci" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=lourenci" title="Tests">⚠️</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/marcosvega91"><img src="https://avatars2.githubusercontent.com/u/5365582?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Marco Moretti</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=marcosvega91" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=marcosvega91" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/ybentz"><img src="https://avatars3.githubusercontent.com/u/14811577?v=4?s=100" width="100px;" alt=""/><br /><sub><b>ybentz</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=ybentz" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=ybentz" title="Tests">⚠️</a></td>
    <td align="center"><a href="http://www.lemoncode.net/"><img src="https://avatars2.githubusercontent.com/u/4374977?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Nasdan</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3ANasdan" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/JavierMartinz"><img src="https://avatars1.githubusercontent.com/u/1155507?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Javier Martínez</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=JavierMartinz" title="Documentation">📖</a></td>
    <td align="center"><a href="http://www.visualjerk.de"><img src="https://avatars0.githubusercontent.com/u/28823153?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jörg Bayreuther</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=visualjerk" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=visualjerk" title="Tests">⚠️</a> <a href="https://github.com/testing-library/user-event/commits?author=visualjerk" title="Documentation">📖</a></td>
    <td align="center"><a href="https://ko-fi.com/thislucas"><img src="https://avatars0.githubusercontent.com/u/8645841?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Lucas Bernalte</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=lucbpz" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/maxnewlands"><img src="https://avatars3.githubusercontent.com/u/1304166?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Maxwell Newlands</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=maxnewlands" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=maxnewlands" title="Tests">⚠️</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/ph-fritsche"><img src="https://avatars3.githubusercontent.com/u/39068198?v=4?s=100" width="100px;" alt=""/><br /><sub><b>ph-fritsche</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=ph-fritsche" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=ph-fritsche" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/reywright"><img src="https://avatars3.githubusercontent.com/u/708820?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Rey Wright</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Areywright" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=reywright" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/mischnic"><img src="https://avatars1.githubusercontent.com/u/4586894?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Niklas Mischkulnig</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=mischnic" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=mischnic" title="Tests">⚠️</a></td>
    <td align="center"><a href="http://pascalduez.me"><img src="https://avatars3.githubusercontent.com/u/335467?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Pascal Duez</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=pascalduez" title="Code">💻</a></td>
    <td align="center"><a href="http://malachi.dev"><img src="https://avatars3.githubusercontent.com/u/10888943?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Malachi Willey</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=malwilley" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=malwilley" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://clarkwinters.com"><img src="https://avatars2.githubusercontent.com/u/40615752?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Clark Winters</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=cwinters8" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/lazytype"><img src="https://avatars1.githubusercontent.com/u/840985?v=4?s=100" width="100px;" alt=""/><br /><sub><b>lazytype</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=lazytype" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=lazytype" title="Tests">⚠️</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://www.linkedin.com/in/luis-takahashi/"><img src="https://avatars0.githubusercontent.com/u/19766035?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Luís Takahashi</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=luistak" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=luistak" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/jesujcastillom"><img src="https://avatars3.githubusercontent.com/u/7827281?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jesu Castillo</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=jesujcastillom" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=jesujcastillom" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://sarahdayan.dev"><img src="https://avatars1.githubusercontent.com/u/5370675?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sarah Dayan</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=sarahdayan" title="Documentation">📖</a></td>
    <td align="center"><a href="http://saul-mirone.github.io/"><img src="https://avatars0.githubusercontent.com/u/10047788?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Mirone</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3ASaul-Mirone" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/amandapouget"><img src="https://avatars3.githubusercontent.com/u/12855692?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Amanda Pouget</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=amandapouget" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/Sonic12040"><img src="https://avatars3.githubusercontent.com/u/21055893?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sonic12040</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=Sonic12040" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=Sonic12040" title="Tests">⚠️</a> <a href="https://github.com/testing-library/user-event/commits?author=Sonic12040" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/gndelia"><img src="https://avatars1.githubusercontent.com/u/352474?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Gonzalo D'Elia</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=gndelia" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=gndelia" title="Tests">⚠️</a> <a href="https://github.com/testing-library/user-event/commits?author=gndelia" title="Documentation">📖</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/vasilii-kovalev"><img src="https://avatars0.githubusercontent.com/u/10310491?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Vasilii Kovalev</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=vasilii-kovalev" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=vasilii-kovalev" title="Documentation">📖</a></td>
    <td align="center"><a href="https://www.daleseo.com"><img src="https://avatars1.githubusercontent.com/u/5466341?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Dale Seo</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=daleseo" title="Documentation">📖</a></td>
    <td align="center"><a href="http://www.alex-boyce.me/"><img src="https://avatars.githubusercontent.com/u/4050934?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Alex Boyce</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=curiosity26" title="Code">💻</a></td>
    <td align="center"><a href="https://benadamstyles.com"><img src="https://avatars.githubusercontent.com/u/4380655?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ben Styles</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=benadamstyles" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=benadamstyles" title="Tests">⚠️</a></td>
    <td align="center"><a href="http://laurabeatris.com"><img src="https://avatars.githubusercontent.com/u/48022589?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Laura Beatris</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=LauraBeatris" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=LauraBeatris" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://twitter.com/boriscoder"><img src="https://avatars.githubusercontent.com/u/812240?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Boris Serdiuk</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Ajust-boris" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://bozdoz.com"><img src="https://avatars.githubusercontent.com/u/1410985?v=4?s=100" width="100px;" alt=""/><br /><sub><b>bozdoz</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=bozdoz" title="Documentation">📖</a> <a href="https://github.com/testing-library/user-event/issues?q=author%3Abozdoz" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=bozdoz" title="Tests">⚠️</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/jKatt"><img src="https://avatars.githubusercontent.com/u/5550790?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jan Kattelans</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=jKatt" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/schoeneu"><img src="https://avatars.githubusercontent.com/u/3261341?v=4?s=100" width="100px;" alt=""/><br /><sub><b>schoeneu</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Aschoeneu" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/mkapal"><img src="https://avatars.githubusercontent.com/u/6420535?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Martin Kapal</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Amkapal" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://gr.linkedin.com/in/bastakis"><img src="https://avatars.githubusercontent.com/u/1146626?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Stavros</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Asstauross" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/geoffroymounier"><img src="https://avatars.githubusercontent.com/u/24386870?v=4?s=100" width="100px;" alt=""/><br /><sub><b>geoffroymounier</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Ageoffroymounier" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://fergusmcdonald.com"><img src="https://avatars.githubusercontent.com/u/3115675?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Fergus McDonald</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=fergusmcdonald" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/robin-ambachtsheer"><img src="https://avatars.githubusercontent.com/u/2611873?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Robin Ambachtsheer</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Arobin-ambachtsheer" title="Bug reports">🐛</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/MohitPopli"><img src="https://avatars.githubusercontent.com/u/17976072?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Mohit</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3AMohitPopli" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=MohitPopli" title="Code">💻</a> <a href="https://github.com/testing-library/user-event/commits?author=MohitPopli" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/InExtremaRes"><img src="https://avatars.githubusercontent.com/u/1635491?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Daniel Contreras</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3AInExtremaRes" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://eugene.coding.blog"><img src="https://avatars.githubusercontent.com/u/13572283?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Eugene Ghanizadeh</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=loreanvictor" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/vicrep"><img src="https://avatars.githubusercontent.com/u/11432241?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Victor Repkow</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=vicrep" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/GreenGremlin"><img src="https://avatars.githubusercontent.com/u/647452?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jonathan Felchlin</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=GreenGremlin" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/sydneyjodon-wk"><img src="https://avatars.githubusercontent.com/u/51122966?v=4?s=100" width="100px;" alt=""/><br /><sub><b>sydneyjodon-wk</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Asydneyjodon-wk" title="Bug reports">🐛</a> <a href="https://github.com/testing-library/user-event/commits?author=sydneyjodon-wk" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/codepath2019"><img src="https://avatars.githubusercontent.com/u/49729798?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Charles Magic Woo</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Acodepath2019" title="Bug reports">🐛</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/mkurcius"><img src="https://avatars.githubusercontent.com/u/1613212?v=4?s=100" width="100px;" alt=""/><br /><sub><b>mkurcius</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=mkurcius" title="Code">💻</a></td>
    <td align="center"><a href="http://stderr.timfischbach.de"><img src="https://avatars.githubusercontent.com/u/26554?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Tim Fischbach</b></sub></a><br /><a href="https://github.com/testing-library/user-event/issues?q=author%3Atf" title="Bug reports">🐛</a></td>
    <td align="center"><a href="https://github.com/eventualbuddha"><img src="https://avatars.githubusercontent.com/u/1938?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Brian Donovan</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=eventualbuddha" title="Code">💻</a></td>
    <td align="center"><a href="http://www.largetimber.com"><img src="https://avatars.githubusercontent.com/u/10626756?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Eric Wang</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=fa93hws" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/jesperorb"><img src="https://avatars.githubusercontent.com/u/21122051?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jesper Orb</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=jesperorb" title="Code">💻</a></td>
    <td align="center"><a href="https://johannesfischer.github.io/"><img src="https://avatars.githubusercontent.com/u/28100?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Johannes Fischer</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=JohannesFischer" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/777PolarFox777"><img src="https://avatars.githubusercontent.com/u/19393384?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Andrew D.</b></sub></a><br /><a href="https://github.com/testing-library/user-event/commits?author=777PolarFox777" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors][all-contributors] specification.
Contributions of any kind welcome!

## LICENSE

[MIT](LICENSE)

<!-- prettier-ignore-start -->
[npm]: https://www.npmjs.com
[node]: https://nodejs.org
[build-badge]: https://img.shields.io/github/workflow/status/testing-library/user-event/validate/main?logo=github&style=flat-square
[build]: https://github.com/testing-library/user-event/actions?query=workflow%3Avalidate
[coverage-badge]: https://img.shields.io/codecov/c/github/testing-library/user-event.svg?style=flat-square
[coverage]: https://codecov.io/github/testing-library/user-event
[version-badge]: https://img.shields.io/npm/v/@testing-library/user-event.svg?style=flat-square
[package]: https://www.npmjs.com/package/@testing-library/user-event
[downloads-badge]: https://img.shields.io/npm/dm/@testing-library/user-event.svg?style=flat-square
[npmtrends]: http://www.npmtrends.com/@testing-library/user-event
[license-badge]: https://img.shields.io/npm/l/@testing-library/user-event.svg?style=flat-square
[license]: https://github.com/testing-library/user-event/blob/main/LICENSE
[prs-badge]: https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square
[prs]: http://makeapullrequest.com
[coc-badge]: https://img.shields.io/badge/code%20of-conduct-ff69b4.svg?style=flat-square
[coc]: https://github.com/testing-library/user-event/blob/main/other/CODE_OF_CONDUCT.md
[github-watch-badge]: https://img.shields.io/github/watchers/testing-library/user-event.svg?style=social
[github-watch]: https://github.com/testing-library/user-event/watchers
[github-star-badge]: https://img.shields.io/github/stars/testing-library/user-event.svg?style=social
[github-star]: https://github.com/testing-library/user-event/stargazers
[twitter]: https://twitter.com/intent/tweet?text=Check%20out%20user-event%20by%20%40@TestingLib%20https%3A%2F%2Fgithub.com%2Ftesting-library%2Fuser-event%20%F0%9F%91%8D
[twitter-badge]: https://img.shields.io/twitter/url/https/github.com/testing-library/user-event.svg?style=social
[emojis]: https://github.com/all-contributors/all-contributors#emoji-key
[all-contributors]: https://github.com/all-contributors/all-contributors
[all-contributors-badge]: https://img.shields.io/github/all-contributors/testing-library/user-event?color=orange&style=flat-square
[guiding-principle]: https://twitter.com/kentcdodds/status/977018512689455106
[bugs]: https://github.com/testing-library/user-event/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+sort%3Acreated-desc+label%3Abug
[requests]: https://github.com/testing-library/user-event/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+sort%3Areactions-%2B1-desc+label%3Aenhancement
[good-first-issue]: https://github.com/testing-library/user-event/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+sort%3Areactions-%2B1-desc+label%3Aenhancement+label%3A%22good+first+issue%22
[fire-event]: https://testing-library.com/docs/dom-testing-library/api-events#fireevent
[discord-badge]: https://img.shields.io/discord/723559267868737556.svg?color=7389D8&labelColor=6A7EC2&logo=discord&logoColor=ffffff&style=flat-square
[discord]: https://discord.gg/testing-library
[stackoverflow]: https://stackoverflow.com/questions/tagged/user-event
<!-- prettier-ignore-end -->

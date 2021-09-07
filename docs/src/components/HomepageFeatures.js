import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Easy to use',
    description: (
      <>
        Nebula has almost no dependency to deploy a cluster.
        Just config data connections and enjoy its power through its analytical UI.
      </>
    ),
  },
  {
    title: 'Focus on real time',
    description: (
      <>
        Nebula allows you to connect most popular real time streaming data like Kafka.
        Just a few lines of configuration and see everything happens through its smart visualizations.

        It supports semi-realtime if you data is landed in batch on cloud storage like S3, Google cloud storage...
      </>
    ),
  },
  {
    title: 'Advanced by sdk',
    // Svg: require('../../static/img/build.svg').default,
    description: (
      <>
        Advanced users could leverage its javascript SDK.
        With the SDK, you can either create instant UDF to execute on the server side.
        Or you can manipulate data results for different metrics on the client side.
      </>
    ),
  },
];

function Feature({ Svg, title, description }) {
  return (
    <div className={clsx('col col--4')}>
      {/* <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div> */}
      <div className="text--left padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}

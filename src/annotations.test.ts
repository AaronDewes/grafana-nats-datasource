import { AnnotationQuery } from '@grafana/data';

import { annotationSupport, prepareAnnotationQuery } from './annotations';
import { AnnotationQueryTypes, MyQuery, QueryTypeOptions, QueryTypes } from './types';

function annotation(target?: Partial<MyQuery>): AnnotationQuery<MyQuery> {
  return {
    name: 'test',
    enable: true,
    iconColor: 'red',
    target: target as MyQuery,
  };
}

describe('annotation support', () => {
  it('is wired up with an editor and a default query', () => {
    expect(annotationSupport.QueryEditor).toBeDefined();
    expect(annotationSupport.prepareQuery).toBe(prepareAnnotationQuery);
    expect(annotationSupport.getDefaultQuery).toBeDefined();
  });

  it('offers a default query that can produce annotations', () => {
    const query = annotationSupport.getDefaultQuery!();

    expect(AnnotationQueryTypes).toContain(query.queryType!);
    // the example script has to show the columns Grafana maps to annotation fields
    expect(query.jsFn).toContain('time');
    expect(query.requestTimeout).toBeTruthy();
    // the default must survive prepareQuery, otherwise a new annotation silently does nothing
    expect(prepareAnnotationQuery(annotation(query))).toBeTruthy();
  });

  it.each(AnnotationQueryTypes)('runs a %s query', (queryType) => {
    const target = { queryType, natsSubject: 'some.subject' };

    expect(prepareAnnotationQuery(annotation(target))).toEqual(target);
  });

  it('skips a streaming query, which can never complete', () => {
    expect(prepareAnnotationQuery(annotation({ queryType: 'SUBSCRIBE' }))).toBeUndefined();
  });

  it('skips an annotation without a query', () => {
    expect(prepareAnnotationQuery(annotation(undefined))).toBeUndefined();
  });

  it('only leaves out query types that cannot produce annotations', () => {
    const allQueryTypes = QueryTypeOptions.map((option) => option.value!);
    const excluded = allQueryTypes.filter((queryType: QueryTypes) => !AnnotationQueryTypes.includes(queryType));

    // if a new query type is added, it has to be classified deliberately
    expect(excluded).toEqual(['SUBSCRIBE']);
  });
});

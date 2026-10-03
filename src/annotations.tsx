import React from 'react';
import { AnnotationQuery, AnnotationSupport } from '@grafana/data';

import { QueryEditor } from './components/QueryEditor';
import { AnnotationQueryTypes, DEFAULT_ANNOTATION_QUERY, MyQuery } from './types';

/**
 * prepareAnnotationQuery turns the stored annotation into the query that is sent to the backend.
 * Returning undefined skips the query.
 */
export function prepareAnnotationQuery(annotation: AnnotationQuery<MyQuery>): MyQuery | undefined {
    const query = annotation.target;
    if (!query) {
        return undefined;
    }
    // A subscription never completes, so it cannot produce annotations. The editor does not offer
    // it, but a dashboard edited by hand or built before this existed may still contain one.
    if (!AnnotationQueryTypes.includes(query.queryType)) {
        return undefined;
    }
    return query;
}

/**
 * annotationSupport makes the data source usable in annotation queries.
 *
 * Grafana builds the annotation events from the returned data frame: a "time" column is required,
 * and "timeEnd", "title", "text" and "tags" are used when present. Columns can also be mapped
 * explicitly in the annotation editor.
 */
export const annotationSupport: AnnotationSupport<MyQuery> = {
    getDefaultQuery: () => DEFAULT_ANNOTATION_QUERY,

    prepareQuery: prepareAnnotationQuery,

    // The regular query editor is reused, minus the query types that cannot produce annotations.
    QueryEditor: (props) => <QueryEditor {...props} availableQueryTypes={AnnotationQueryTypes} />,
};

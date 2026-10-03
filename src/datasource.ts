import { DataSourceInstanceSettings, CoreApp } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { MyQuery, MyDataSourceOptions, DEFAULT_QUERY } from './types';
import { annotationSupport } from './annotations';

export class DataSource extends DataSourceWithBackend<MyQuery, MyDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<MyDataSourceOptions>) {
    super(instanceSettings);
    // makes the data source selectable in a dashboard's annotation queries
    this.annotations = annotationSupport;
  }

  getDefaultQuery(_: CoreApp): Partial<MyQuery> {
    return DEFAULT_QUERY
  }
}

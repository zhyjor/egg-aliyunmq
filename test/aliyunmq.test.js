'use strict';

const mock = require('egg-mock');

describe('test/aliyunmq.test.js', () => {
  let app;
  before(() => {
    app = mock.app({
      baseDir: 'apps/aliyunmq-test',
    });
    return app.ready();
  });

  after(() => app.close());
  afterEach(mock.restore);

  it('should GET /', () => {
    return app.httpRequest()
      .get('/')
      .expect('hi, aliyunmq')
      .expect(200);
  });
});

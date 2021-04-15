'use strict';
module.exports = app => {
  const ctx = app.createAnonymousContext({});
  app.messenger.on('@egg-aliyunmq/consumer', async ({ topic, options, payload }) => {
    const { fileNameWithMethod, serviceFolderName } = options;
    const [ service, method ] = fileNameWithMethod.split('.');
    ctx.runInBackground(async () => {
      ctx.service[serviceFolderName][service][method](topic, payload);
    });
  });
};

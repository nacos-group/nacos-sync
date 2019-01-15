const base = require('./webpack.base.conf')

module.exports = Object.assign({}, base, {
    devServer: {
        port: 9010,
        proxy: [{
            context: ['/nacossync'],
            changeOrigin: true,
            secure: false,
            target: 'http://30.5.121.153:8081'
        }],
        disableHostCheck: true
    },
    mode: 'development'
})

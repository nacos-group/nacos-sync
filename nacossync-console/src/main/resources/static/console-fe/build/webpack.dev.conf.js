const base = require('./webpack.base.conf')

module.exports = Object.assign({}, base, {
    devServer: {
        port: 9010,
        proxy: [{
            context: ['/nacossync'],
            changeOrigin: true,
            secure: false,
            target: 'http://10.101.15.150:8081'
        }],
        disableHostCheck: true
    },
    mode: 'development'
})

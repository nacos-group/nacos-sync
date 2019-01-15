const path = require('path')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const MiniCssExtractPlugin = require("mini-css-extract-plugin")

const isDev = process.env.NODE_ENV !== 'production'

module.exports = {
    entry: {
        main: './src/index.js'
    },
    output: {
        filename: './js/[name].[chunkhash:8].js',
        path: path.resolve(__dirname, '../dist')
    },
    module: {
        rules: [{
            test: /\.scss$/,
            use: [
                isDev ? "style-loader" : MiniCssExtractPlugin.loader,
                "css-loader",
                "sass-loader"
            ]
        }, {
            enforce: 'pre',
            test: /\.(js|jsx)$/,
            exclude: /node_modules/,
            loader: 'eslint-loader',
        }, {
            test: /\.(js|jsx)$/,
            exclude: /node_modules/,
            use: ['babel-loader']
        }, {
            test: [/\.bmp$/, /\.gif$/, /\.jpe?g$/, /\.png$/],
            loader: 'url-loader',
            options: {
                limit: 10000,
                name: '/images/[name].[hash:8].[ext]',
            },
        }, {
            test: /\.(ttf|woff|svg)$/,
            use: [{
                loader: 'url-loader',
                options: {
                    name: '/fonts/[name].[hash:8].[ext]'
                }
            }],

        }]
    },
    plugins: [
        new HtmlWebpackPlugin({
            filename: 'index.html',
            template: './src/index.html',
            minify: !isDev
        })
    ]
};

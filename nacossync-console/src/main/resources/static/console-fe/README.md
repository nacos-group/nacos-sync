# Console 前端工程

> 这是一个前端工程，构建后的页面将输出到 skywalker-console module 中

## 前端框架选型
1. React
2. axios
3. redux
4. fusion
5. webpack

## 开发

> 准备工作
- `git clone git@gitlab.alibaba-inc.com:middleware-config/skywalker.git`
- `cd skywalker`

> 开始
1. 创建软连接
    ```
       ln -s ./skywalker-console/src/main/resources/static/console-fe ./
       cd console-fe
       ...
    ```
2. `yarn(npm install)` ➡️ `npm run dev`
3. 构建页面 `npm run build`


## 注意

构建输出目录为前端工程目录下的 `dist/` ， `run build` 时会复制到上层目录，所以不能在上层目录（
`skywalker/skywalker-console/src/main/resources/static/`） 下出现以下内容

1. `js` directory
2. `css` directory
3. `images` directory
4. `fonts` directory
5. `index.html` file

__这些目录和文件，会在项目构建后，复制前，清空和删除__

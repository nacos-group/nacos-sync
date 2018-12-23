import React from 'react';
import ReactDOM from 'react-dom';
import { createStore, combineReducers, compose, applyMiddleware } from 'redux';
import { routerReducer } from 'react-router-redux';
import thunk from 'redux-thunk';
import { Provider, connect } from 'react-redux';
import { HashRouter, Route, Switch, Redirect } from 'react-router-dom';
import { ConfigProvider } from '@alifd/next';

import Layout from './containers/Layout';
import ServiceSync from './containers/ServiceSync';
import SystemConfig from './containers/SystemConfig';
import ClusterConfig from './containers/ClusterConfig';
import { LANGUAGE_KEY, REDUX_DEVTOOLS } from './constants';

import * as reducers from './reducers';
import { changeLanguage } from './reducers/locale';

import './index.scss';

if (!localStorage.getItem(LANGUAGE_KEY)) {
  localStorage.setItem(LANGUAGE_KEY, navigator.language === 'zh-CN' ? 'zh-CN' : 'en-US');
}

const reducer = combineReducers({
  ...reducers,
  routing: routerReducer,
});

const store = createStore(
  reducer,
  compose(
    applyMiddleware(thunk),
    window[REDUX_DEVTOOLS] ? window[REDUX_DEVTOOLS]() : f => f,
  ),
);

@connect(state => ({ ...state.locale }), { changeLanguage })
class App extends React.Component {
  componentDidMount() {
    const language = localStorage.getItem(LANGUAGE_KEY);
    this.props.changeLanguage(language);
  }

  generateRouter() {
    return (
      <HashRouter>
        <Layout>
          <Switch>
            <Route path="/" exact render={() => <Redirect to="/serviceSync" />} />
            <Route path="/serviceSync" component={ServiceSync} />
            <Route path="/systemConfig" component={SystemConfig} />
            <Route path="/clusterConfig" component={ClusterConfig} />
          </Switch>
        </Layout>
      </HashRouter>
    );
  }

  render() {
    const { locale } = this.props;
    return (
      <ConfigProvider locale={locale}>
        {this.generateRouter()}
      </ConfigProvider>
    );
  }
}

ReactDOM.render(<Provider store={store}><App /></Provider>, document.getElementById('root'));

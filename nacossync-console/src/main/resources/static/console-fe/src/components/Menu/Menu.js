import React from 'react';
import { withRouter } from 'react-router-dom';
import { ConfigProvider, Nav } from '@alifd/next';
import './index.scss';

const { Item, SubNav } = Nav;

@withRouter
@ConfigProvider.config
class Menu extends React.Component {
  static displayName = 'Menu'

  _getSubNav(menuItem) {
    return (
      <SubNav label={menuItem.title} key={menuItem.id}>
        {menuItem.child.map(item => (item.child ? this._getSubNav(item) : this._getItem(item)))}
      </SubNav>
    );
  }

  _getItem(menuItem) {
    return (
      <Item
        key={menuItem.id}
        onSelect={() => {
          if (this.props.location.pathname === menuItem.link) return;
          this.props.history.push(menuItem.link);
        }}
      >
        {menuItem.title}
      </Item>
    );
  }

  generateMenu() {
    return this.buildMenuData().map(item => (item.child ? this._getSubNav(item) : this._getItem(item)));
  }

  buildMenuData() {
    const { locale = {} } = this.props;
    const { serviceSync, clusterConfig, systemConfig } = locale;
    return [{
      id: 1,
      title: serviceSync,
      link: '/serviceSync',
    }, {
      id: 2,
      title: clusterConfig,
      link: '/clusterConfig',
    }, {
      id: 3,
      title: systemConfig,
      link: '/systemConfig',
    }];
  }

  render() {
    return (
      <div className="menu">
        <Nav openMode="multiple">{this.generateMenu()}</Nav>
      </div>
    );
  }
}

export default Menu;

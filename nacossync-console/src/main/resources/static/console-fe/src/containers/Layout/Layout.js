import React from 'react';
import { Grid } from '@alifd/next';
import Header from '../../components/Header';
import Menu from '../../components/Menu';
import './index.scss';

const { Row, Col } = Grid;

class Layout extends React.Component {
  render() {
    return (
      <div className="containers">
        <Header />
        <Row className="layout">
          <Col fixedSpan="9" className="nav-bar">
            <h1 className="title">Nacos-Sync 0.4.8</h1>
            <Menu />
          </Col>
          <Col className="main-panel">{this.props.children}</Col>
        </Row>
      </div>
    );
  }
}

export default Layout;

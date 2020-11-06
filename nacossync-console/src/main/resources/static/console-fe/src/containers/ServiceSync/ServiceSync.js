import React from 'react';
import { connect } from 'react-redux';
import { Table, Pagination, Form, Input, Button, Dialog, Message, ConfigProvider } from '@alifd/next';
import FuncHead from '../../components/FuncHead';
import AddSyncDialog from './AddSyncDialog';
import { list, update, deleteRow } from '../../reducers/task';
import { list as getClusterList } from '../../reducers/cluster';
import './index.scss';

const FormItem = Form.Item;

@connect(state => ({ ...state.task, clusterModels: state.cluster.clusterModels }), { list, getClusterList })
@ConfigProvider.config
class ServiceSync extends React.Component {
  static displayName = 'ServiceSync'

  constructor(props) {
    super(props);
    this.addDialog = React.createRef();
    this.state = {
      pageNum: 1,
      loading: true,
      search: {
        serviceName: '',
      },
    };
  }

  componentDidMount() {
    const { pageNum } = this.state;
    this.props.list({ pageNum }).then(() => this.loadComplete());
    this.props.getClusterList({ pageSize: 10000000, pageNum: 1 });
  }

  loadComplete() {
    this.setState({ loading: false });
  }

  turnPage(pageNum) {
    const { search } = this.state;
    this.setState({
      pageNum,
      loading: true,
    }, () => this.props.list({ pageNum, ...search }).then(() => this.loadComplete()));
  }

  deleteServiceSync(record) {
    const { taskId } = record;
    const { locale = {} } = this.props;
    Dialog.confirm({
      title: locale.confirm,
      content: locale.confirmMsg,
      onOk: () => deleteRow({ taskId }).then(() => {
        this.turnPage(this.state.pageNum);
        Message.success(locale.deleteSuccessMsg);
      }),
    });
  }

  suspendedServiceSync(record) {
    const { taskId } = record;
    const { locale = {} } = this.props;
    Dialog.confirm({
      title: locale.confirm,
      content: locale.suspendedMsg,
      onOk: () => update({ taskId, taskStatus: 'DELETE' }).then(() => {
        this.turnPage(this.state.pageNum);
        Message.success(locale.successMsg);
      }),
    });
  }

  resynchronize(record) {
    const { taskId } = record;
    const { locale = {} } = this.props;
    update({ taskId, taskStatus: 'SYNC' }).then(() => {
      this.turnPage(this.state.pageNum);
      Message.success(locale.syncSuccessMsg);
    });
  }

  onChangeSearchForm(obj) {
    const { search } = this.state;
    this.setState({ search: Object.assign({}, search, obj) });
  }

  openAddDialog() {
    this.addDialog.current.getWrappedInstance().getInstance().open();
  }

  render() {
    const { loading, pageNum, search } = this.state;
    const { taskModels = [], locale = {}, totalSize = 0, totalPage = 0, clusterModels = [] } = this.props;
    const clusterMap = {};
    clusterModels.forEach(({ clusterId, clusterName }) => {
      clusterMap[clusterId] = clusterName;
    });
    return (
      <div className="service-sync">
        <FuncHead title={locale.title} />
        <Form inline className="search-form">
          <FormItem label={`${locale.serviceName}:`}>
            <Input
              style={{ width: 198 }}
              value={search.serviceName}
              placeholder={locale.serviceNamePlaceholder}
              onChange={serviceName => this.onChangeSearchForm({ serviceName })}
            />
          </FormItem>
          <Button type="primary" onClick={() => this.turnPage(1)}>{locale.search}</Button>
          <Button
            type="normal"
            className="add-btn"
            onClick={() => this.openAddDialog()}
          >{locale.addSync}
          </Button>
        </Form>
        <Table dataSource={taskModels} loading={loading}>
          <Table.Column title={locale.serviceName} dataIndex="serviceName" />
          <Table.Column title={locale.groupName} dataIndex="groupName" />
          <Table.Column title={locale.nameSpace} dataIndex="nameSpace" />
          <Table.Column
            title={locale.sourceCluster}
            dataIndex="sourceClusterId"
            cell={key => clusterMap[key]}
          />
          <Table.Column
            title={locale.destCluster}
            dataIndex="destClusterId"
            cell={key => clusterMap[key]}
          />
          <Table.Column
            title={locale.operation}
            cell={(value, index, record) => {
              const buttonList = [];
              if (record.taskStatus === 'SYNC') {
                buttonList.push(
                  <Button
                    key="suspendedBtn"
                    text
                    type="primary"
                    onClick={() => this.suspendedServiceSync(record)}
                  >{locale.suspendedBtn}
                  </Button>,
                );
              }
              if (record.taskStatus === 'DELETE') {
                buttonList.push(
                  <Button
                    key="resynchronizeBtn"
                    text
                    type="primary"
                    onClick={() => this.resynchronize(record)}
                  >{locale.resynchronizeBtn}
                  </Button>,
                );
                buttonList.push(
                  <Button
                    key="deleteBtn"
                    text
                    type="primary"
                    style={{ marginLeft: 18 }}
                    onClick={() => this.deleteServiceSync(record)}
                  >{locale.deleteBtn}
                  </Button>,
                );
              }
              return buttonList;
            }}
          />
        </Table>
        {
          totalPage > 1 && (
            <Pagination
              onChange={currentPageNum => this.turnPage(currentPageNum)}
              current={pageNum}
              total={totalSize}
              className="list-pagination"
            />
          )
        }
        <AddSyncDialog ref={this.addDialog} turnPage={pn => this.turnPage(pn)} />
      </div>
    );
  }
}

export default ServiceSync;

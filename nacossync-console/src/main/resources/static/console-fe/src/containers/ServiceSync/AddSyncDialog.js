import React from 'react';
import { Form, Input, Select, Dialog, ConfigProvider } from '@alifd/next';
import '../../style/dialog-form.scss';
import connect from 'react-redux/es/connect/connect';
import { add } from '../../reducers/task';
import { list } from '../../reducers/cluster';

const FormItem = Form.Item;

@connect(state => ({ ...state.cluster }), { list }, null, { withRef: true })
@ConfigProvider.config
class AddSyncDialog extends React.Component {
  static displayName = 'AddSyncDialog'

  constructor(props) {
    super(props);
    this.state = {
      visible: false,
      destClusterId: '',
      groupName: '',
      serviceName: '',
      sourceClusterId: '',
    };
  }

  componentDidMount() {
    this.props.list({ pageSize: 10000000, pageNum: 1 });
  }

  save() {
    const { destClusterId, groupName, serviceName, sourceClusterId } = this.state;
    add({ destClusterId, groupName, serviceName, sourceClusterId })
      .then(() => {
        this.props.turnPage(1);
        this.close();
      })
      .catch(() => this.close());
  }

  close() {
    this.setState({ visible: false });
  }

  open = () => this.setState({ visible: true })

  render() {
    const { locale = {}, clusterModels = [] } = this.props;
    return (
      <Dialog
        className="dialog-form"
        title={locale.title}
        visible={this.state.visible}
        onOk={() => this.save()}
        onCancel={() => this.close()}
        onClose={() => this.close()}
      >
        <Form>
          <FormItem label={`${locale.serviceName}:`}>
            <Input
              placeholder={locale.serviceNamePlaceholder}
              onChange={serviceName => this.setState({ serviceName })}
            />
          </FormItem>
          <FormItem label={`${locale.groupName}:`}>
            <Input
              placeholder={locale.groupNamePlaceholder}
              onChange={groupName => this.setState({ groupName })}
            />
          </FormItem>
          <FormItem label={`${locale.sourceCluster}:`}>
            <Select onChange={sourceClusterId => this.setState({ sourceClusterId })}>
              {
                clusterModels.map(({ clusterId, clusterName }) => (
                  <Select.Option key={clusterId} value={clusterId}>{clusterName}</Select.Option>
                ))
              }
            </Select>
          </FormItem>
          <FormItem label={`${locale.destCluster}:`}>
            <Select onChange={destClusterId => this.setState({ destClusterId })}>
              {
                clusterModels.map(({ clusterId, clusterName }) => (
                  <Select.Option key={clusterId} value={clusterId}>{clusterName}</Select.Option>
                ))
              }
            </Select>
          </FormItem>
        </Form>
      </Dialog>
    );
  }
}

export default AddSyncDialog;

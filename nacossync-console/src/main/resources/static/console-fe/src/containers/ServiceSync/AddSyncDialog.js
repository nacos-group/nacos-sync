import React from 'react';
import { Form, Input, Select, Dialog, ConfigProvider } from '@alifd/next';
import '../../style/dialog-form.scss';
import connect from 'react-redux/es/connect/connect';
import { add } from '../../reducers/task';
import { list } from '../../reducers/cluster';

const FormItem = Form.Item;
const { Option } = Select;

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
      nameSpace: '',
      serviceName: '',
      sourceClusterId: '',
      version: '',
      sourceCluster: {},
    };
  }

  componentDidMount() {
    this.props.list({ pageSize: 10000000, pageNum: 1 });
  }

  save() {
    const { destClusterId, nameSpace, groupName, serviceName, sourceClusterId, version } = this.state;
    add({ destClusterId, nameSpace, groupName, serviceName, sourceClusterId, version })
      .then(() => {
        this.props.turnPage(1);
        this.close();
      })
      .catch(() => this.close());
  }

  close() {
    this.setState({ visible: false, sourceCluster: {} });
  }

  onSourceClusterChange(sourceClusterId) {
    const [sourceCluster] = this.props.clusterModels.filter(({ clusterId }) => clusterId === sourceClusterId);
    this.setState({ sourceClusterId, sourceCluster });
  }

  open = () => this.setState({ visible: true })

  render() {
    const { sourceCluster } = this.state;
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
          {
            sourceCluster.clusterType === 'ZK' && (
              <FormItem label={`${locale.version}:`}>
                <Input
                  placeholder={locale.versionPlaceholder}
                  onChange={version => this.setState({ version })}
                />
              </FormItem>
            )
          }
          <FormItem label={`${locale.sourceCluster}:`}>
            <Select onChange={value => this.onSourceClusterChange(value)}>
              {
                clusterModels.map(({ clusterId, clusterName }) => (
                  <Option key={clusterId} value={clusterId}>{clusterName}</Option>
                ))
              }
            </Select>
          </FormItem>
          <FormItem label={`${locale.destCluster}:`}>
            <Select onChange={destClusterId => this.setState({ destClusterId })}>
              {
                clusterModels.map(({ clusterId, clusterName }) => (
                  <Option key={clusterId} value={clusterId}>{clusterName}</Option>
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

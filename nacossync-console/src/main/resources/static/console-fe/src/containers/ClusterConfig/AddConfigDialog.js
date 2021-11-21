import React from 'react';
import { Form, Input, Select, Dialog, ConfigProvider } from '@alifd/next';
import { connect } from 'react-redux';
import { getTypes, add } from '../../reducers/cluster';
import '../../style/dialog-form.scss';

const FormItem = Form.Item;
const { Option } = Select;

@connect(state => ({ ...state.cluster }), { getTypes }, null, { withRef: true })
@ConfigProvider.config
class AddConfigDialog extends React.Component {
    static displayName = 'AddConfigDialog'

    constructor(props) {
      super(props);
      this.state = {
        visible: false,
        clusterName: '',
        clusterType: '',
        namespace: '',
        password: '',
        userName: '',
        connectKeyList: [],
      };
    }

    componentDidMount() {
      this.props.getTypes();
    }

    save() {
      const { clusterName, namespace, userName, password, clusterType, connectKeyList } = this.state;
      add({ clusterName, namespace, userName, password, clusterType, connectKeyList })
        .then(() => {
          this.props.turnPage(1);
          this.close();
        })
        .catch(() => this.close());
    }

    close() {
      this.setState({
        visible: false,
        clusterType: '',
      });
    }

    open = () => this.setState({ visible: true })

    render() {
      const { types = [], locale = {} } = this.props;
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
            <FormItem label={`${locale.clusterName}:`}>
              <Input
                onChange={clusterName => this.setState({ clusterName })}
                placeholder={locale.clusterNamePlaceholder}
              />
            </FormItem>
            <FormItem label={`${locale.clusterType}:`}>
              <Select
                onChange={clusterType => this.setState({ clusterType })}
              >
                {
                                types.map(type => (
                                  <Option value={type} key={type}>{type}</Option>
                                ))
                            }
              </Select>
            </FormItem>
            {
              this.state.clusterType === 'NACOS' && (
              <>
                <FormItem
                  label={`${locale.namespace}:`}
                >
                  <Input
                    placeholder={locale.namespacePlaceholder}
                    onChange={ns => this.setState({ namespace: ns })}
                  />
                </FormItem>
                <FormItem
                  label={`${locale.username}:`}
                >
                  <Input
                    placeholder={locale.usernamePlaceholder}
                    onChange={un => this.setState({ userName: un })}
                  />
                </FormItem>
                <FormItem
                  label={`${locale.password}:`}
                >
                  <Input.Password
                    placeholder={locale.passwordPlaceholder}
                    onChange={pw => this.setState({ password: pw })}
                  />
                </FormItem>
              </>)
            }
            <FormItem label={`${locale.connectKeyList}:`}>
              <Input.TextArea
                onChange={(connectKeyListStr) => {
                  this.setState({ connectKeyList: connectKeyListStr.split('\n') });
                }}
                placeholder={locale.connectKeyListPlaceholder}
              />
            </FormItem>
          </Form>
        </Dialog>
      );
    }
}

export default AddConfigDialog;

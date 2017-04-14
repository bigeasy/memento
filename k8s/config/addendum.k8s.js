var interfaces = require('os').networkInterfaces()
var address = interfaces.en1.filter(function (iface) {
    return iface.family == 'IPv4'
}).shift().address

console.log(JSON.stringify({
    kind: 'Deployment',
    apiVersion: 'extensions/v1beta1',
    metadata: {
        name: 'addendum',
        namespace: 'addendum',
        labels: {
            name: 'addendum',
            environment: 'minikube'
        }
    },
    spec: {
        replicas: 1,
        selector: {
            matchLabels: {
                name: 'addendum',
                environment: 'minikube'
            }
        },
        template: {
            metadata: {
                labels: {
                    name: 'addendum',
                    environment: 'minikube'
                }
            },
            spec: {
                restartPolicy: 'Always',
                terminationGracePeriodSeconds: 5,
                containers: [{
                    name: 'logger',
                    image: 'homeport/image-addendum:latest',
                    imagePullPolicy: 'Never',
                    command: [ '/usr/local/bin/service/logger' ],
                    env: [{
                        name: 'ADDENDUM_LOGGER_AUTHORITY',
                        value: address + ':8514'
                    }],
                    volumeMounts: [{
                        mountPath: '/run', name: 'rw', subPath: 'run/logger'
                    }, {
                        mountPath: '/tmp', name: 'rw', subPath: 'tmp/logger'
                    }]
                }, {
                    name: 'discovery',
                    image: 'homeport/image-addendum:latest',
                    imagePullPolicy: 'Never',
                    command: [ '/usr/local/bin/service/discovery' ],
                    volumeMounts: [{
                        mountPath: '/run', name: 'rw', subPath: 'run/logger'
                    }, {
                        mountPath: '/tmp', name: 'rw', subPath: 'tmp/logger'
                    }]
                }, {
                    name: 'chaperon',
                    image: 'homeport/image-addendum:latest',
                    imagePullPolicy: 'Never',
                    command: [ '/usr/local/bin/service/chaperon' ],
                    volumeMounts: [{
                        mountPath: '/run', name: 'rw', subPath: 'run/logger'
                    }, {
                        mountPath: '/tmp', name: 'rw', subPath: 'tmp/logger'
                    }]
                }, {
                    name: 'conduit',
                    image: 'homeport/image-addendum:latest',
                    imagePullPolicy: 'Never',
                    command: [ '/usr/local/bin/service/conduit' ],
                    ports: [{ name: 'conduit', containerPort: 8486 }],
                    volumeMounts: [{
                        mountPath: '/run', name: 'rw', subPath: 'run/logger'
                    }, {
                        mountPath: '/tmp', name: 'rw', subPath: 'tmp/logger'
                    }]
                }],
                volumes: [{ name: 'rw', emptyDir: {} }]
            }
        }
    }
}, null, 4))

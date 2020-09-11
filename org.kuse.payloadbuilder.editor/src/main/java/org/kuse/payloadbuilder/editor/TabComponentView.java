/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.editor;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.plaf.basic.BasicButtonUI;

/** View for tab header component */
class TabComponentView extends JPanel
{
    private final JLabel lblTitle;
    
    TabComponentView(String title, Icon icon)
    {
        this(title, icon, null);
    }
    
    TabComponentView(String title, Icon icon, final Runnable closeAction)
    {
        super(new FlowLayout(FlowLayout.LEFT, 0, 0));
        setOpaque(false);
        setBorder(BorderFactory.createEmptyBorder(2, 0, 0, 0));

        lblTitle = new JLabel(title, icon, SwingConstants.CENTER);
        lblTitle.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 5));
        add(lblTitle);

        if (closeAction != null)
        {
            JButton button = new TabButton();
            button.addActionListener(evt -> closeAction.run());
            add(button);
        }

    }
    
    public void setTitle(String titleString)
    {
        lblTitle.setText(titleString);
    }

    class TabButton extends JButton// implements ActionListener
    {
        public TabButton()
        {
            int size = 17;
            setPreferredSize(new Dimension(size, size));
            //            setToolTipText("close this tab");
            //Make the button looks the same for all Laf's
            setUI(new BasicButtonUI());
            //Make it transparent
            setContentAreaFilled(false);
            //No need to be focusable
            setFocusable(false);
            setBorder(BorderFactory.createEtchedBorder());
            setBorderPainted(false);
            //Making nice rollover effect
            //we use the same listener for all buttons
            addMouseListener(buttonMouseListener);
            setRolloverEnabled(true);
            //Close the proper tab by clicking the button
            //            addActionListener(this);
        }

        //        @Override
        //        public void actionPerformed(ActionEvent e)
        //        {
        //            int i = pane.indexOfTabComponent(ButtonTabComponent.this);
        //            if (i != -1)
        //            {
        //                pane.remove(i);
        //            }
        //        }

        //we don't want to update UI for this button
        @Override
        public void updateUI()
        {
        }

        //paint the cross
        @Override
        protected void paintComponent(Graphics g)
        {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g.create();
            //shift the image for pressed buttons
            if (getModel().isPressed())
            {
                g2.translate(1, 1);
            }
            g2.setStroke(new BasicStroke(2));
            g2.setColor(Color.BLACK);
            if (getModel().isRollover())
            {
                g2.setColor(Color.MAGENTA);
            }
            int delta = 6;
            g2.drawLine(delta, delta, getWidth() - delta - 1, getHeight() - delta - 1);
            g2.drawLine(getWidth() - delta - 1, delta, delta, getHeight() - delta - 1);
            g2.dispose();
        }
    }

    private final static MouseListener buttonMouseListener = new MouseAdapter()
    {
        @Override
        public void mouseEntered(MouseEvent e)
        {
            Component component = e.getComponent();
            if (component instanceof AbstractButton)
            {
                AbstractButton button = (AbstractButton) component;
                button.setBorderPainted(true);
            }
        }

        @Override
        public void mouseExited(MouseEvent e)
        {
            Component component = e.getComponent();
            if (component instanceof AbstractButton)
            {
                AbstractButton button = (AbstractButton) component;
                button.setBorderPainted(false);
            }
        }
    };
}
